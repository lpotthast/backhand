//! The threads that decompress the data blocks of one image

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::thread::JoinHandle;

use crate::error::BackhandError;
use crate::kinds::Kind;
use crate::traits::types::Compressor;

/// How a filesystem reader decompresses the data blocks of its files
///
/// With one thread, a file's reader decompresses each block on the thread that reads the file.
/// With more, the filesystem reader owns that many threads, which decompress the blocks of all its
/// files, and each file's reader keeps up to that many blocks in flight.
pub struct Decompression {
    threads: NonZeroUsize,
    /// `None` with one thread
    workers: Option<DecompressionThreads>,
}

impl Decompression {
    /// Spawns `threads` threads that decompress blocks of `block_size` bytes, unless `threads` is
    /// one
    pub(crate) fn new(
        threads: NonZeroUsize,
        kind: &Kind,
        compressor: Option<Compressor>,
        block_size: u32,
    ) -> Result<Self, BackhandError> {
        let workers = if threads.get() == 1 {
            None
        } else {
            Some(DecompressionThreads::spawn(threads, kind, compressor, block_size)?)
        };
        Ok(Self { threads, workers })
    }

    /// The number of blocks that a file's reader keeps in flight
    pub(crate) fn threads(&self) -> NonZeroUsize {
        self.threads
    }

    /// The threads that decompress blocks, or `None` if the reading thread decompresses them
    pub(crate) fn workers(&self) -> Option<&DecompressionThreads> {
        self.workers.as_ref()
    }
}

/// A block's compressed bytes, and the decompressed bytes once a thread decompressed it
pub(crate) struct DecompressedBlock {
    /// The compressed bytes, handed back so that their buffer can be reused
    pub(crate) compressed: Vec<u8>,
    pub(crate) decompressed: Result<Vec<u8>, BackhandError>,
}

/// A block for a decompression thread, and where to send the result
struct Job {
    compressed: Vec<u8>,
    /// An empty buffer for the decompressed bytes
    decompressed: Vec<u8>,
    result: mpsc::SyncSender<DecompressedBlock>,
}

/// Threads that decompress blocks until they are dropped
pub(crate) struct DecompressionThreads {
    /// `None` only while dropping, which ends the threads
    jobs: Option<mpsc::Sender<Job>>,
    handles: Vec<JoinHandle<()>>,
}

impl DecompressionThreads {
    fn spawn(
        threads: NonZeroUsize,
        kind: &Kind,
        compressor: Option<Compressor>,
        block_size: u32,
    ) -> Result<Self, BackhandError> {
        let (jobs, job_receiver) = mpsc::channel::<Job>();
        let job_receiver = Arc::new(Mutex::new(job_receiver));
        // Threads that were spawned before an error end when `Self` is dropped
        let mut decompression_threads = Self { jobs: Some(jobs), handles: Vec::new() };
        for _ in 0..threads.get() {
            let job_receiver = Arc::clone(&job_receiver);
            let kind = kind.clone();
            let handle = std::thread::Builder::new()
                .name("backhand-decompression".into())
                .spawn(move || decompress_jobs(&job_receiver, &kind, compressor, block_size))?;
            decompression_threads.handles.push(handle);
        }
        Ok(decompression_threads)
    }

    /// Starts decompressing `compressed` into the empty buffer `decompressed`
    ///
    /// The receiver gets the block once it is decompressed. It gets nothing if the thread
    /// panicked.
    pub(crate) fn decompress(
        &self,
        compressed: Vec<u8>,
        decompressed: Vec<u8>,
    ) -> Result<mpsc::Receiver<DecompressedBlock>, BackhandError> {
        let (result, receiver) = mpsc::sync_channel(1);
        let job = Job { compressed, decompressed, result };
        let jobs = self.jobs.as_ref().expect("the sender of jobs exists until dropping");
        // Sending fails only once every thread panicked
        jobs.send(job).map_err(|_| {
            BackhandError::InternalState(err_text!("the decompression threads panicked"))
        })?;
        Ok(receiver)
    }
}

impl Drop for DecompressionThreads {
    fn drop(&mut self) {
        // Without a sender, each thread ends once the jobs that were sent are done
        self.jobs = None;
        for handle in self.handles.drain(..) {
            // A thread that panicked already failed the reads that waited for it
            let _ = handle.join();
        }
    }
}

/// Decompresses the blocks of `jobs` until the sender of jobs is dropped
fn decompress_jobs(
    jobs: &Mutex<mpsc::Receiver<Job>>,
    kind: &Kind,
    compressor: Option<Compressor>,
    block_size: u32,
) {
    loop {
        // A thread that panicked while holding the lock left the receiver intact
        let job = jobs.lock().unwrap_or_else(PoisonError::into_inner).recv();
        let Ok(Job { compressed, mut decompressed, result }) = job else { return };
        decompressed.reserve(block_size as usize);
        let decompressed = kind
            .decompress(&compressed, &mut decompressed, compressor, block_size as usize)
            .map(|()| decompressed);
        // A file's reader that was dropped or seeked no longer waits for the block
        let _ = result.send(DecompressedBlock { compressed, decompressed });
    }
}
