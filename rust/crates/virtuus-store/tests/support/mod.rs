use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use virtuus_store::{
    ObjectPage, ObjectStore, Result, Scope, StoreError, StoredObject, WriteCondition,
};

#[derive(Default)]
pub struct Gate {
    pub entered: Notify,
    pub release: Notify,
}

pub struct FaultStore {
    pub inner: Arc<dyn ObjectStore>,
    pub gate_put: Mutex<Option<Arc<Gate>>>,
    pub gate_delete: Mutex<Option<Arc<Gate>>>,
    pub fail_events: AtomicUsize,
    pub fail_reads: AtomicUsize,
    pub fail_receipts: AtomicUsize,
    pub fail_uploads: AtomicUsize,
    pub uncertain_uploads: AtomicUsize,
    pub puts: AtomicUsize,
    pub deletes: AtomicUsize,
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            gate_put: Mutex::new(None),
            gate_delete: Mutex::new(None),
            fail_events: AtomicUsize::new(0),
            fail_reads: AtomicUsize::new(0),
            fail_receipts: AtomicUsize::new(0),
            fail_uploads: AtomicUsize::new(0),
            uncertain_uploads: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
        }
    }
}

fn consume(counter: &AtomicUsize) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    while let Some(next) = remaining.checked_sub(1) {
        match counter.compare_exchange(remaining, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
    false
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn get(&self, s: &Scope, p: &str) -> Result<Option<StoredObject>> {
        if consume(&self.fail_reads) {
            return Err(StoreError::Unavailable("injected read failure".into()));
        }
        self.inner.get(s, p).await
    }
    async fn get_bounded(
        &self,
        s: &Scope,
        p: &str,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>> {
        if consume(&self.fail_reads) {
            return Err(StoreError::Unavailable("injected read failure".into()));
        }
        self.inner.get_bounded(s, p, max_bytes).await
    }

    async fn list(&self, s: &Scope, p: &str, c: Option<&str>, l: usize) -> Result<ObjectPage> {
        self.inner.list(s, p, c, l).await
    }
    async fn put(
        &self,
        s: &Scope,
        p: &str,
        b: &[u8],
        t: &str,
        c: WriteCondition,
        o: Option<&str>,
    ) -> Result<StoredObject> {
        if p.starts_with("project/events/") && consume(&self.fail_events) {
            return Err(StoreError::Unavailable("injected event rejection".into()));
        }
        if p.starts_with("project/issues/") {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate_put.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if consume(&self.fail_uploads) {
                return Err(StoreError::Unavailable(
                    "injected unapplied upload failure".into(),
                ));
            }
            if consume(&self.uncertain_uploads) {
                return Err(StoreError::Uncertain("injected ambiguous upload".into()));
            }
        }
        self.inner.put(s, p, b, t, c, o).await
    }
    async fn delete(&self, s: &Scope, p: &str, r: &str) -> Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate_delete.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.inner.delete(s, p, r).await
    }
    async fn get_control(&self, s: &Scope, p: &str) -> Result<Option<StoredObject>> {
        self.inner.get_control(s, p).await
    }
    async fn put_control(
        &self,
        s: &Scope,
        p: &str,
        b: &[u8],
        c: WriteCondition,
    ) -> Result<StoredObject> {
        if p.starts_with("receipts/") && consume(&self.fail_receipts) {
            return Err(StoreError::Unavailable("injected receipt rejection".into()));
        }
        self.inner.put_control(s, p, b, c).await
    }
}
