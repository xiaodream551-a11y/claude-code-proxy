use std::sync::Arc;
use std::time::Duration;

pub(crate) const CONCURRENCY: usize = 2;
pub(crate) const QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

static WORKERS: once_cell::sync::Lazy<Arc<tokio::sync::Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(tokio::sync::Semaphore::new(CONCURRENCY)));

#[derive(Debug)]
pub(crate) enum TokenCountAdmissionError {
    QueueTimeout,
    Closed,
    WorkerFailed(tokio::task::JoinError),
}

pub(crate) async fn run<T, F>(work: F) -> Result<T, TokenCountAdmissionError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    run_with(WORKERS.clone(), QUEUE_TIMEOUT, work).await
}

pub(crate) async fn run_with<T, F>(
    workers: Arc<tokio::sync::Semaphore>,
    queue_timeout: Duration,
    work: F,
) -> Result<T, TokenCountAdmissionError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = match tokio::time::timeout(queue_timeout, workers.acquire_owned()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return Err(TokenCountAdmissionError::Closed),
        Err(_) => return Err(TokenCountAdmissionError::QueueTimeout),
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(TokenCountAdmissionError::WorkerFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn shared_admission_is_bounded_to_two() {
        assert_eq!(CONCURRENCY, 2);
        assert_eq!(QUEUE_TIMEOUT, Duration::from_secs(30));
        let workers = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
        let first = workers.clone().try_acquire_owned().unwrap();
        let second = workers.clone().try_acquire_owned().unwrap();
        assert!(workers.clone().try_acquire_owned().is_err());
        drop((first, second));
        assert_eq!(workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn saturated_admission_times_out_and_panics_are_reported() {
        let saturated = run_with(
            Arc::new(tokio::sync::Semaphore::new(0)),
            Duration::from_millis(10),
            || 1_u64,
        )
        .await
        .expect_err("a gate with no permits must time out");
        assert!(matches!(saturated, TokenCountAdmissionError::QueueTimeout));

        let failed = run_with(
            Arc::new(tokio::sync::Semaphore::new(1)),
            Duration::from_secs(1),
            || -> u64 { panic!("synthetic token-count worker failure") },
        )
        .await
        .expect_err("a panicking blocking worker must return JoinError");
        assert!(matches!(failed, TokenCountAdmissionError::WorkerFailed(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_work_does_not_stall_tokio_timers() {
        let started = Arc::new(AtomicBool::new(false));
        let started_in_worker = started.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let task = tokio::spawn(run_with(
            Arc::new(tokio::sync::Semaphore::new(1)),
            Duration::from_secs(1),
            move || {
                started_in_worker.store(true, Ordering::Release);
                release_rx.recv().unwrap();
                1_u64
            },
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::time::sleep(Duration::from_millis(5)),
        )
        .await
        .expect("Tokio timer should run while tokenization is blocked");
        release_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }
}
