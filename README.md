# broadcast-ingestor

A Rust microservice that receives [OTLP JSON traces](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding) via HTTP webhook and persists them to Google Cloud Storage.

## Architecture

```
Webhook --POST /v1/traces--> [axum handler] --try_send--> [bounded channel] --recv--> [worker pool] --PUT--> GCS
                               (202 Accepted)              (10k default)              (32 default)
```

The HTTP handler is decoupled from GCS writes via a bounded `tokio::sync::mpsc` channel. The handler returns `202 Accepted` immediately after enqueuing. Worker tasks upload asynchronously. If the channel is full, `503 Service Unavailable` is returned as backpressure.

Failed uploads are placed in an in-memory retry queue with exponential backoff (up to 5 attempts), capped at a configurable memory limit.

## GCS Object Path

```
<YYYY-MM-DD>/<user-id>/<unixtime>-<session-id>/<unixtime>-<trace-id>.json
```

Fields are extracted from the first OTLP span:

| Field | Source | Fallback |
|---|---|---|
| date | `startTimeUnixNano` | current time |
| user-id | `user.id` attribute | `unknown_user` |
| session-id | `session.id` attribute | `no_session` |
| trace-id | `traceId` | random UUID |

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | *(optional)* | GCS bucket name. If unset, the server starts but discards traces. |
| `LISTEN_ADDR` | `0.0.0.0:8080` | Listen address |
| `WORKER_COUNT` | `32` | Number of upload workers |
| `CHANNEL_CAPACITY` | `10000` | Bounded channel size |
| `WEBHOOK_SECRET` | *(disabled)* | Optional bearer token for auth |
| `RETRY_BUFFER_MB` | `500` | Max memory for retry queue |
| `RETRY_INTERVAL_SECS` | `10` | Retry sweep interval |
| `RUST_LOG` | `info` | Log level filter |

## Endpoints

### `GET /health`

Returns `200 OK`. No auth required. Use for liveness/readiness probes.

### `POST /v1/traces`

Accepts OTLP JSON trace payloads.

| Header | Behavior |
|---|---|
| `X-Test-Connection: true` | Returns `200 OK` without processing |
| `Authorization: Bearer <token>` | Required if `WEBHOOK_SECRET` is set |

**Responses:**

- `200` - Test connection acknowledged
- `202` - Trace accepted and queued
- `400` - Invalid JSON
- `401` - Missing or invalid bearer token
- `503` - Backpressure (channel full)

## Local Development

### Prerequisites

- Rust 1.85+ (the `object_store` crate requires edition 2024)
- Docker (for container builds)

### Build and Test

```bash
cargo build --release
cargo test
```

### Run Locally (no GCS)

The server starts without any env vars. Traces are accepted and silently drained.

```bash
cargo run

# In another terminal:
curl http://localhost:8080/health
# -> 200

curl -X POST http://localhost:8080/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[]}'
# -> 202
```

### Run Locally (with GCS)

```bash
# Authenticate with GCP (Application Default Credentials)
gcloud auth application-default login

GCS_BUCKET=my-bucket cargo run
```

## Docker

### Build

```bash
# Native build
docker build -t broadcast-ingestor .

# Cross-compile for linux/amd64 (e.g. from macOS)
docker buildx build --platform linux/amd64 -t broadcast-ingestor --load .
```

The image uses a multi-stage build:
- **Build stage:** `rust:1.93.1-slim` -- compiles with full LTO, single codegen unit, stripped binary
- **Runtime stage:** `chainguard/glibc-dynamic:latest` -- minimal, no shell, no package manager

### Run

```bash
docker run -p 8080:8080 broadcast-ingestor                          # no GCS, health-check only
docker run -p 8080:8080 -e GCS_BUCKET=my-bucket broadcast-ingestor  # with GCS
```

When running on GCE/GKE, the workload identity or attached service account provides credentials automatically. For local Docker runs against real GCS, mount your ADC:

```bash
docker run -p 8080:8080 \
  -e GCS_BUCKET=my-bucket \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  broadcast-ingestor
```

## Deployment

### GCP Prerequisites

1. **GCS Bucket** -- create a bucket for trace storage:
   ```bash
   gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=us-east1
   ```

2. **Artifact Registry Repository** -- create a Docker repo to push images to:
   ```bash
   gcloud artifacts repositories create broadcast-ingestor-dev \
     --repository-format=docker \
     --location=us-east1 \
     --description="broadcast-ingestor dev images"
   ```

3. **Service Account** -- the workload needs `roles/storage.objectCreator` on the bucket:
   ```bash
   # If using a dedicated SA:
   gcloud iam service-accounts create broadcast-ingestor

   gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET_NAME \
     --member="serviceAccount:broadcast-ingestor@YOUR_PROJECT.iam.gserviceaccount.com" \
     --role="roles/storage.objectCreator"
   ```

4. **Workload Identity (GKE)** -- bind the Kubernetes SA to the GCP SA:
   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     broadcast-ingestor@YOUR_PROJECT.iam.gserviceaccount.com \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:YOUR_PROJECT.svc.id.goog[NAMESPACE/KSA_NAME]"
   ```

### CI/CD

The GitHub Actions workflow at `.github/workflows/dev-build-push.yml` builds and pushes on every push to `dev` (or via manual dispatch):

- Authenticates to GCP via Workload Identity Federation
- Builds `linux/amd64` with Docker Buildx + GitHub Actions cache
- Pushes to Google Artifact Registry with tags: `<short-sha>` and `latest`

**Required GitHub repo setup for CI:**

1. Configure [Workload Identity Federation](https://github.com/google-github-actions/auth#workload-identity-federation) between your GitHub repo and GCP project
2. Grant the CI service account these roles:
   - `roles/artifactregistry.writer` on the Artifact Registry repository
3. Update the env vars in the workflow file to match your GCP project and repository names

### Kubernetes / GKE

Minimal deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: broadcast-ingestor
spec:
  replicas: 2
  selector:
    matchLabels:
      app: broadcast-ingestor
  template:
    metadata:
      labels:
        app: broadcast-ingestor
    spec:
      serviceAccountName: broadcast-ingestor  # bound to GCP SA via Workload Identity
      containers:
        - name: broadcast-ingestor
          image: us-east1-docker.pkg.dev/YOUR_PROJECT/broadcast-ingestor-dev/broadcast-ingestor-dev:latest
          ports:
            - containerPort: 8080
          env:
            - name: GCS_BUCKET
              value: "YOUR_BUCKET_NAME"
            - name: WEBHOOK_SECRET
              valueFrom:
                secretKeyRef:
                  name: broadcast-ingestor
                  key: webhook-secret
                  optional: true
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 2
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 2
            periodSeconds: 5
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              memory: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: broadcast-ingestor
spec:
  selector:
    app: broadcast-ingestor
  ports:
    - port: 80
      targetPort: 8080
  type: ClusterIP
```

### Cloud Run

```bash
gcloud run deploy broadcast-ingestor \
  --image=us-east1-docker.pkg.dev/YOUR_PROJECT/broadcast-ingestor-dev/broadcast-ingestor-dev:latest \
  --region=us-east1 \
  --port=8080 \
  --set-env-vars="GCS_BUCKET=YOUR_BUCKET_NAME" \
  --service-account=broadcast-ingestor@YOUR_PROJECT.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --min-instances=1 \
  --max-instances=10
```

## Graceful Shutdown

On `SIGTERM` or `CTRL+C`:

1. Stop accepting new requests
2. Drain the channel (workers finish in-flight uploads)
3. Flush the retry queue (one final attempt per entry)
4. Exit

Kubernetes sends `SIGTERM` by default. The default 30s termination grace period is sufficient for most workloads. Increase `terminationGracePeriodSeconds` if the channel/retry queue is routinely large.

## Tuning

| Scenario | Adjustment |
|---|---|
| High throughput, bursty | Increase `CHANNEL_CAPACITY` (e.g. `50000`) |
| Slow GCS / high latency | Increase `WORKER_COUNT` (e.g. `64`) |
| Memory constrained | Decrease `RETRY_BUFFER_MB` (e.g. `100`) |
| Reduce log noise | Set `RUST_LOG=warn` or `RUST_LOG=trace_ingestor=info` |
