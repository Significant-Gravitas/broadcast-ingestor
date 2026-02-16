use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

pub struct RetryEntry {
    pub path: String,
    pub data: Bytes,
    pub session_id: String,
    pub attempts: u32,
    pub retry_after: Instant,
}

impl RetryEntry {
    fn byte_size(&self) -> usize {
        self.data.len() + self.path.len() + self.session_id.len()
    }
}

pub struct RetryQueue {
    entries: VecDeque<RetryEntry>,
    total_bytes: usize,
    max_bytes: usize,
    session_counts: HashMap<String, usize>,
}

impl RetryQueue {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            total_bytes: 0,
            max_bytes,
            session_counts: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[allow(dead_code)]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Push a failed upload into the retry queue.
    /// If over capacity, evict oldest entries preferring sessions with >1 entry.
    pub fn push(&mut self, entry: RetryEntry) {
        let size = entry.byte_size();
        *self.session_counts.entry(entry.session_id.clone()).or_insert(0) += 1;
        self.total_bytes += size;
        self.entries.push_back(entry);

        while self.total_bytes > self.max_bytes && !self.entries.is_empty() {
            if !self.evict_from_multi_session() {
                // All sessions have only 1 entry; fall back to plain FIFO.
                self.evict_front();
            }
        }
    }

    /// Evict the oldest entry from a session that has >1 entry in the queue.
    /// Returns false if no such session exists.
    fn evict_from_multi_session(&mut self) -> bool {
        let pos = self.entries.iter().position(|e| {
            self.session_counts.get(&e.session_id).copied().unwrap_or(0) > 1
        });
        match pos {
            Some(idx) => {
                self.remove_at(idx);
                true
            }
            None => false,
        }
    }

    fn evict_front(&mut self) {
        self.remove_at(0);
    }

    fn remove_at(&mut self, idx: usize) {
        if let Some(entry) = self.entries.remove(idx) {
            self.total_bytes = self.total_bytes.saturating_sub(entry.byte_size());
            let count = self.session_counts.entry(entry.session_id.clone()).or_insert(1);
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.session_counts.remove(&entry.session_id);
            }
            tracing::warn!(
                path = %entry.path,
                attempts = entry.attempts,
                "evicted retry entry (buffer full)"
            );
        }
    }

    /// Pop all entries whose retry time has passed (from the front).
    pub fn pop_ready(&mut self) -> Vec<RetryEntry> {
        let now = Instant::now();
        let mut ready = Vec::new();
        while let Some(front) = self.entries.front() {
            if front.retry_after <= now {
                let entry = self.entries.pop_front().unwrap();
                self.total_bytes = self.total_bytes.saturating_sub(entry.byte_size());
                let count = self.session_counts.entry(entry.session_id.clone()).or_insert(1);
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.session_counts.remove(&entry.session_id);
                }
                ready.push(entry);
            } else {
                break;
            }
        }
        ready
    }

    /// Drain all remaining entries (for graceful shutdown).
    pub fn drain_all(&mut self) -> Vec<RetryEntry> {
        self.total_bytes = 0;
        self.session_counts.clear();
        self.entries.drain(..).collect()
    }
}

/// Compute the next retry delay with exponential backoff.
pub fn retry_delay(attempts: u32) -> Duration {
    let secs = 2u64.saturating_pow(attempts).min(60);
    Duration::from_secs(secs)
}

pub const MAX_ATTEMPTS: u32 = 5;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(session: &str, size: usize, attempts: u32) -> RetryEntry {
        RetryEntry {
            path: "test/path.json".to_string(),
            data: Bytes::from(vec![0u8; size]),
            session_id: session.to_string(),
            attempts,
            retry_after: Instant::now(),
        }
    }

    #[test]
    fn push_and_pop_ready() {
        let mut q = RetryQueue::new(10_000);
        q.push(make_entry("s1", 100, 0));
        q.push(make_entry("s2", 100, 0));
        assert_eq!(q.len(), 2);

        let ready = q.pop_ready();
        assert_eq!(ready.len(), 2);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn eviction_prefers_multi_session() {
        // Max 500 bytes
        let mut q = RetryQueue::new(500);
        // s1 has 2 entries
        q.push(make_entry("s1", 100, 0));
        q.push(make_entry("s1", 100, 0));
        // s2 has 1 entry
        q.push(make_entry("s2", 100, 0));

        // This push should trigger eviction. s1 has >1 so its oldest gets evicted.
        q.push(make_entry("s3", 200, 0));

        // s2 should still be present (only had 1 entry)
        let sessions: Vec<_> = q.entries.iter().map(|e| e.session_id.as_str()).collect();
        assert!(sessions.contains(&"s2"), "s2 should be preserved");
    }

    #[test]
    fn fifo_fallback_when_all_single() {
        let mut q = RetryQueue::new(300);
        q.push(make_entry("s1", 100, 0));
        q.push(make_entry("s2", 100, 0));
        q.push(make_entry("s3", 100, 0));

        // This triggers eviction; all sessions have 1 entry, so FIFO evicts s1.
        q.push(make_entry("s4", 100, 0));

        let sessions: Vec<_> = q.entries.iter().map(|e| e.session_id.as_str()).collect();
        assert!(!sessions.contains(&"s1"), "s1 (oldest) should be evicted");
        assert!(sessions.contains(&"s4"), "s4 (newest) should be present");
    }

    #[test]
    fn retry_delay_exponential() {
        assert_eq!(retry_delay(0), Duration::from_secs(1));
        assert_eq!(retry_delay(1), Duration::from_secs(2));
        assert_eq!(retry_delay(2), Duration::from_secs(4));
        assert_eq!(retry_delay(5), Duration::from_secs(32));
        assert_eq!(retry_delay(10), Duration::from_secs(60)); // capped
    }

    #[test]
    fn drain_all() {
        let mut q = RetryQueue::new(10_000);
        q.push(make_entry("s1", 100, 0));
        q.push(make_entry("s2", 100, 0));
        let drained = q.drain_all();
        assert_eq!(drained.len(), 2);
        assert_eq!(q.len(), 0);
        assert_eq!(q.total_bytes(), 0);
    }
}
