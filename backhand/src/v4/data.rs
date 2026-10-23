//! File Data

use no_std_io2::io::{Read, Seek, SeekFrom, Write};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Mutex, PoisonError, mpsc};

use deku::prelude::*;
use solana_nohash_hasher::IntMap;
use xxhash_rust::xxh64::Xxh64;

use crate::error::BackhandError;
use crate::v4::filesystem::writer::FilesystemCompressor;
use crate::v4::fragment::Fragment;
use crate::v4::reader::WriteSeek;

use crate::v4::filesystem::reader::SquashfsRawData;

// bitflag for data size field in inode for signifying that the data is uncompressed
const DATA_STORED_UNCOMPRESSED: u32 = 1 << 24;

#[derive(Copy, Clone, Debug, PartialEq, Eq, DekuRead, DekuWrite, DekuSize)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
pub struct DataSize(u32);
impl DataSize {
    #[inline]
    pub fn new(size: u32, uncompressed: bool) -> Self {
        let mut value: u32 = size;
        if value > DATA_STORED_UNCOMPRESSED {
            panic!("value is too big");
        }
        if uncompressed {
            value |= DATA_STORED_UNCOMPRESSED;
        }
        Self(value)
    }

    #[inline]
    pub fn new_compressed(size: u32) -> Self {
        Self::new(size, false)
    }

    #[inline]
    pub fn new_uncompressed(size: u32) -> Self {
        Self::new(size, true)
    }

    /// A block of all zeros, with no bytes stored in the image
    #[inline]
    pub const fn hole() -> Self {
        Self(0)
    }

    #[inline]
    pub fn uncompressed(&self) -> bool {
        self.0 & DATA_STORED_UNCOMPRESSED != 0
    }

    #[inline]
    pub fn set_uncompressed(&mut self) {
        self.0 |= DATA_STORED_UNCOMPRESSED
    }

    #[inline]
    pub fn set_compressed(&mut self) {
        self.0 &= !DATA_STORED_UNCOMPRESSED
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.0 & !DATA_STORED_UNCOMPRESSED
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Added {
    // Only Data was added. `sparse` is the number of bytes stored as holes.
    Data { blocks_start: u64, block_sizes: Vec<DataSize>, sparse: u64 },
    // Only Fragment was added
    Fragment { frag_index: u32, block_offset: u32 },
}

/// A block of data at the start of a buffer of the block size
///
/// The buffer keeps its length, so that reading the next block into it needs no initialization.
struct RawBlock {
    buffer: Vec<u8>,
    len: usize,
}

impl RawBlock {
    fn bytes(&self) -> &[u8] {
        &self.buffer[..self.len]
    }

    /// Reads from `reader` until the buffer is full or `reader` ends
    fn read(&mut self, reader: &mut impl Read) -> std::io::Result<()> {
        use std::io::ErrorKind;
        self.len = 0;
        while self.len < self.buffer.len() {
            match reader.read(&mut self.buffer[self.len..]) {
                Ok(0) => break,
                Ok(n) => self.len += n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// The compressor of a [`DataWriter`]
pub(crate) type DataCompressor<'a> = &'a (
        dyn crate::traits::CompressionAction<
    Compressor = super::compressor::Compressor,
    FilesystemCompressor = FilesystemCompressor,
    SuperBlock = super::squashfs::SuperBlock,
    Error = crate::BackhandError,
> + Send
            + Sync
    );

pub(crate) struct DataWriter<'a> {
    compressor: DataCompressor<'a>,
    block_size: u32,
    fs_compressor: FilesystemCompressor,
    /// If some, cache of HashMap<file_len, HashMap<hash of whole file, (file_len, Added)>>
    #[allow(clippy::type_complexity)]
    dup_cache: Option<IntMap<u64, IntMap<u64, (usize, Added)>>>,
    /// Un-written fragment_bytes
    pub(crate) fragment_bytes: Vec<u8>,
    pub(crate) fragment_table: Vec<Fragment>,
}

impl<'a> DataWriter<'a> {
    pub fn new(
        compressor: DataCompressor<'a>,
        fs_compressor: FilesystemCompressor,
        block_size: u32,
        no_duplicate_files: bool,
    ) -> Self {
        Self {
            compressor,
            block_size,
            fs_compressor,
            dup_cache: no_duplicate_files.then_some(HashMap::default()),
            fragment_bytes: Vec::with_capacity(block_size as usize),
            fragment_table: vec![],
        }
    }

    /// Add to data writer, either a pre-compressed Data or Fragment
    // TODO: support tail-end fragments (off by default in squashfs-tools/mksquashfs)
    pub(crate) fn just_copy_it<W: WriteSeek>(
        &mut self,
        mut reader: SquashfsRawData,
        mut writer: W,
    ) -> Result<(usize, Added), BackhandError> {
        //just clone it, because block sizes where never modified, just copy it
        let source = reader.file;
        let mut block_sizes = source.block_sizes().to_vec();
        let sparse = source.sparse();
        let mut read_buf = vec![];
        let mut decompress_buf = vec![];

        // if the first block is not full (fragment), store only a fragment
        // otherwise processed to store blocks
        let blocks_start = writer.stream_position()?;
        // Older backhand versions gave an empty file a fragment, do not copy that
        if source.file_len() == 0 {
            return Ok((0, Added::Data { blocks_start, block_sizes: vec![], sparse: 0 }));
        }
        let first_block = match reader.next_block(&mut read_buf) {
            Some(Ok(first_block)) => first_block,
            Some(Err(x)) => return Err(x),
            None => return Ok((0, Added::Data { blocks_start, block_sizes, sparse })),
        };

        // write and early return if fragment
        if first_block.fragment {
            reader.decompress(first_block, &mut read_buf, &mut decompress_buf)?;
            // if this doesn't fit in the current fragment bytes
            // compress the current fragment bytes and add to data_bytes
            if (decompress_buf.len() + self.fragment_bytes.len()) > self.block_size as usize {
                self.finalize(writer)?;
            }
            // add to fragment bytes
            let frag_index = self.fragment_table.len() as u32;
            let block_offset = self.fragment_bytes.len() as u32;
            self.fragment_bytes.write_all(&decompress_buf)?;

            return Ok((decompress_buf.len(), Added::Fragment { frag_index, block_offset }));
        }

        // The reader gives a hole as a block of zeros, but a hole has no bytes in the image
        let mut is_hole = source.block_sizes().iter().map(|size| size.size() == 0);
        let mut copy_block = |writer: &mut W, raw: &[u8]| -> Result<(), BackhandError> {
            if is_hole.next() != Some(true) {
                writer.write_all(raw)?;
            }
            Ok(())
        };

        copy_block(&mut writer, &read_buf)?;
        while let Some(block) = reader.next_block(&mut read_buf) {
            let block = block?;
            if block.fragment {
                reader.decompress(block, &mut read_buf, &mut decompress_buf)?;
                // TODO: support tail-end fragments, for now just treat it like a block
                let cb = self.compressor.compress(
                    &decompress_buf,
                    self.fs_compressor,
                    self.block_size,
                )?;
                // compression didn't reduce size
                if cb.len() > decompress_buf.len() {
                    // store uncompressed
                    block_sizes.push(DataSize::new_uncompressed(decompress_buf.len() as u32));
                    writer.write_all(&decompress_buf)?;
                } else {
                    // store compressed
                    block_sizes.push(DataSize::new_compressed(cb.len() as u32));
                    writer.write_all(&cb)?;
                }
            } else {
                copy_block(&mut writer, &read_buf)?;
            }
        }
        let file_size = source.file_len();
        Ok((file_size, Added::Data { blocks_start, block_sizes, sparse }))
    }

    /// Runs `add` with a [`DataSession`] that writes the data of files to `writer`, and returns
    /// the size and location of each file that `add` added, in the order `add` added them.
    ///
    /// The session compresses blocks on `threads` threads, and on the calling thread if `threads`
    /// is one. It reads files and writes `writer` only on the calling thread, in the order the
    /// files were added, so the written data is the same for any number of threads.
    pub(crate) fn write_files<W: WriteSeek>(
        &mut self,
        threads: NonZeroUsize,
        writer: W,
        add: impl FnOnce(&mut DataSession<'_, 'a, W>) -> Result<(), BackhandError>,
    ) -> Result<Vec<(usize, Added)>, BackhandError> {
        if threads.get() == 1 {
            let mut session = DataSession::new(self, writer, None);
            add(&mut session)?;
            return session.finish();
        }
        let (compressor, fs_compressor, block_size) =
            (self.compressor, self.fs_compressor, self.block_size);
        let (jobs, job_receiver) = mpsc::channel::<Job>();
        let job_receiver = Mutex::new(job_receiver);
        std::thread::scope(|scope| {
            for _ in 0..threads.get() {
                let job_receiver = &job_receiver;
                scope.spawn(move || {
                    compress_jobs(job_receiver, compressor, fs_compressor, block_size)
                });
            }
            let compression_threads = CompressionThreads { jobs, threads };
            let mut session = DataSession::new(self, writer, Some(compression_threads));
            add(&mut session)?;
            session.finish()
            // Dropping the session drops the sender of jobs, which ends the threads
        })
    }

    /// Compress the fragments that were under length, write to data, add to fragment table, clear
    /// current fragment_bytes
    pub fn finalize<W: Write + Seek>(&mut self, mut writer: W) -> Result<(), BackhandError> {
        let start = writer.stream_position()?;
        let cb =
            self.compressor.compress(&self.fragment_bytes, self.fs_compressor, self.block_size)?;

        // compression didn't reduce size
        let size = if cb.len() > self.fragment_bytes.len() {
            // store uncompressed
            writer.write_all(&self.fragment_bytes)?;
            DataSize::new_uncompressed(self.fragment_bytes.len() as u32)
        } else {
            // store compressed
            writer.write_all(&cb)?;
            DataSize::new_compressed(cb.len() as u32)
        };
        self.fragment_table.push(Fragment::new(start, size, 0));
        self.fragment_bytes.clear();
        Ok(())
    }
}

/// The blocks that a [`DataSession`] lets wait for compression beyond one for each thread
const EXTRA_COMPRESSING_BLOCKS: usize = 4;

/// A block for a compression thread, and where to send the result
struct Job {
    raw: RawBlock,
    compressed: mpsc::SyncSender<CompressedBlock>,
}

/// A block and its compressed bytes
struct CompressedBlock {
    raw: RawBlock,
    compressed: Result<Vec<u8>, BackhandError>,
}

/// Compresses the blocks of `jobs` until the session drops the sender of jobs
fn compress_jobs(
    jobs: &Mutex<mpsc::Receiver<Job>>,
    compressor: DataCompressor<'_>,
    fs_compressor: FilesystemCompressor,
    block_size: u32,
) {
    loop {
        // A thread that panicked while holding the lock left the receiver intact
        let job = jobs.lock().unwrap_or_else(PoisonError::into_inner).recv();
        let Ok(Job { raw, compressed }) = job else { return };
        let compressed_bytes = compressor.compress(raw.bytes(), fs_compressor, block_size);
        // The session stops waiting when it ends early with an error
        let _ = compressed.send(CompressedBlock { raw, compressed: compressed_bytes });
    }
}

/// The threads that compress the blocks of a [`DataSession`]
struct CompressionThreads {
    jobs: mpsc::Sender<Job>,
    threads: NonZeroUsize,
}

/// A block on its way to the image
enum Block {
    Compressed(CompressedBlock),
    /// A compression thread sends the block once it is compressed, or drops the sender if it
    /// panicked
    Compressing(mpsc::Receiver<CompressedBlock>),
}

impl Block {
    /// Whether the block is compressed, so that writing it does not wait
    fn is_compressed(&mut self) -> Result<bool, BackhandError> {
        if let Block::Compressing(receiver) = self {
            match receiver.try_recv() {
                Ok(block) => *self = Block::Compressed(block),
                Err(mpsc::TryRecvError::Empty) => return Ok(false),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(BackhandError::InternalState(err_text!(
                        "a compression thread panicked"
                    )));
                }
            }
        }
        Ok(true)
    }

    fn wait(self) -> Result<CompressedBlock, BackhandError> {
        match self {
            Block::Compressed(block) => Ok(block),
            Block::Compressing(receiver) => receiver.recv().map_err(|_| {
                BackhandError::InternalState(err_text!("a compression thread panicked"))
            }),
        }
    }
}

/// What a [`DataSession`] writes next, in the order of the image
enum Step {
    /// An empty file, which has no blocks
    EmptyFile { file: usize },
    /// The start of a file that is stored in blocks
    FileStart,
    /// A block of the current file
    Block(Block),
    /// A block of zeros of the current file, stored as a hole
    Hole { len: u64 },
    /// The end of the current file, whose data hashes to `hash`
    FileEnd { file: usize, file_len: usize, hash: u64 },
    /// A block of fragments
    Fragment(Block),
}

/// The file whose blocks a [`DataSession`] writes
struct BlockFile {
    blocks_start: u64,
    block_sizes: Vec<DataSize>,
    sparse: u64,
}

/// Adds files to a [`DataWriter`] (see [`DataWriter::write_files`]).
///
/// Reading a file only plans the writes as [`Step`]s and hands its blocks to the compression
/// threads. The steps are written in their order as soon as their blocks are compressed. At most
/// `max_compressing` blocks wait for that, which bounds the memory the session uses. The buffers
/// of written blocks are reused for the next blocks.
pub(crate) struct DataSession<'d, 'a, W> {
    data: &'d mut DataWriter<'a>,
    writer: W,
    /// `None` compresses blocks on the calling thread
    compression_threads: Option<CompressionThreads>,
    max_compressing: usize,
    /// Blocks handed to compression and not yet written
    compressing: usize,
    steps: VecDeque<Step>,
    /// Buffers of written blocks, to read the next blocks into
    free_buffers: Vec<Vec<u8>>,
    /// Fragment blocks planned so far, including those not yet written
    fragment_count: u32,
    /// The size and location of each file added, once written
    files: Vec<Option<(usize, Added)>>,
    current_file: Option<BlockFile>,
}

impl<'d, 'a, W: WriteSeek> DataSession<'d, 'a, W> {
    fn new(
        data: &'d mut DataWriter<'a>,
        writer: W,
        compression_threads: Option<CompressionThreads>,
    ) -> Self {
        // One block for each thread, and a few more, so that no thread waits while the calling
        // thread writes. More blocks did not write faster, but use more memory.
        let max_compressing = compression_threads
            .as_ref()
            .map_or(1, |threads| threads.threads.get() + EXTRA_COMPRESSING_BLOCKS);
        let fragment_count = data.fragment_table.len() as u32;
        Self {
            data,
            writer,
            compression_threads,
            max_compressing,
            compressing: 0,
            steps: VecDeque::new(),
            free_buffers: vec![],
            fragment_count,
            files: vec![],
            current_file: None,
        }
    }

    /// Adds the data that `reader` reads as the next file, either as Data or as a Fragment
    ///
    /// If the data writer removes duplicates, a file whose data was added before refers to that
    /// data instead.
    // TODO: support tail-end fragments (off by default in squashfs-tools/mksquashfs)
    pub(crate) fn add_bytes(&mut self, mut reader: impl Read) -> Result<(), BackhandError> {
        let file = self.files.len();
        self.files.push(None);
        let block_size = self.data.block_size as usize;
        let mut block = self.raw_block();
        block.read(&mut reader)?;

        // an empty file must carry no fragment reference: a zero-byte
        // fragment entry makes the kernel squashfs driver reject the inode
        // with EINVAL on stat/open (Added::Data with no blocks encodes
        // frag_index 0xffffffff, matching mksquashfs)
        if block.len == 0 {
            self.free_buffers.push(block.buffer);
            return self.plan(Step::EmptyFile { file });
        }

        // block not exactly the size of the block
        if block.len != block_size {
            // if this doesn't fit in the current fragment bytes
            // compress the current fragment bytes and add to data_bytes
            if (block.len + self.data.fragment_bytes.len()) > block_size {
                self.add_fragments()?;
            }

            // add to fragment bytes
            let frag_index = self.fragment_count;
            let block_offset = self.data.fragment_bytes.len() as u32;
            self.data.fragment_bytes.extend_from_slice(block.bytes());

            self.files[file] = Some((block.len, Added::Fragment { frag_index, block_offset }));
            self.free_buffers.push(block.buffer);
            return Ok(());
        }

        // Add to data bytes
        self.plan(Step::FileStart)?;
        let mut hasher = Xxh64::new(0);
        let mut file_len = 0;
        while block.len != 0 {
            hasher.update(block.bytes());
            file_len += block.len;
            if block.bytes().iter().all(|byte| *byte == 0) {
                // like mksquashfs, store a block of zeros as a hole
                self.plan(Step::Hole { len: block.len as u64 })?;
            } else {
                let compressing = self.compress(block);
                self.plan(Step::Block(compressing))?;
                block = self.raw_block();
            }
            block.read(&mut reader)?;
        }
        self.free_buffers.push(block.buffer);
        self.plan(Step::FileEnd { file, file_len, hash: hasher.digest() })
    }

    /// Adds the next file by copying its compressed data from another image (see
    /// [`DataWriter::just_copy_it`])
    pub(crate) fn copy(&mut self, reader: SquashfsRawData) -> Result<(), BackhandError> {
        // `just_copy_it` writes right away, so everything planned before must be written first
        self.flush()?;
        let added = self.data.just_copy_it(reader, &mut self.writer)?;
        self.fragment_count = self.data.fragment_table.len() as u32;
        self.files.push(Some(added));
        Ok(())
    }

    /// Writes everything planned, and returns the size and location of each file added
    fn finish(mut self) -> Result<Vec<(usize, Added)>, BackhandError> {
        self.flush()?;
        self.files
            .into_iter()
            .map(|file| {
                file.ok_or(BackhandError::InternalState(err_text!("file data not written")))
            })
            .collect()
    }

    fn flush(&mut self) -> Result<(), BackhandError> {
        while self.write_next_step()? {}
        Ok(())
    }

    /// A block buffer for reading the next block into
    fn raw_block(&mut self) -> RawBlock {
        let block_size = self.data.block_size as usize;
        let buffer = self.free_buffers.pop().unwrap_or_else(|| vec![0; block_size]);
        RawBlock { buffer, len: 0 }
    }

    /// Plans to write the current fragment bytes as a fragment block
    fn add_fragments(&mut self) -> Result<(), BackhandError> {
        let mut next_fragment_bytes = self.raw_block().buffer;
        next_fragment_bytes.clear();
        let mut buffer = std::mem::replace(&mut self.data.fragment_bytes, next_fragment_bytes);
        let len = buffer.len();
        buffer.resize(self.data.block_size as usize, 0);
        self.fragment_count += 1;
        let compressing = self.compress(RawBlock { buffer, len });
        self.plan(Step::Fragment(compressing))
    }

    /// Compresses `raw`, on a compression thread if the session has them
    fn compress(&mut self, raw: RawBlock) -> Block {
        self.compressing += 1;
        let Some(compression_threads) = &self.compression_threads else {
            let compressed = self.data.compressor.compress(
                raw.bytes(),
                self.data.fs_compressor,
                self.data.block_size,
            );
            return Block::Compressed(CompressedBlock { raw, compressed });
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        // The threads end only after the session, so they still receive
        let _ = compression_threads.jobs.send(Job { raw, compressed: sender });
        Block::Compressing(receiver)
    }

    /// Plans `step`, and writes the steps that are ready. Waits for compression only while too
    /// many blocks are compressing.
    fn plan(&mut self, step: Step) -> Result<(), BackhandError> {
        self.steps.push_back(step);
        while self.compressing >= self.max_compressing {
            self.write_next_step()?;
        }
        while self.next_step_is_ready()? {
            self.write_next_step()?;
        }
        Ok(())
    }

    /// Whether the next step can be written without waiting
    fn next_step_is_ready(&mut self) -> Result<bool, BackhandError> {
        match self.steps.front_mut() {
            None => Ok(false),
            Some(Step::Block(block) | Step::Fragment(block)) => block.is_compressed(),
            Some(_) => Ok(true),
        }
    }

    /// Writes the next step, waiting for its block to be compressed. Returns false if no step is
    /// left.
    fn write_next_step(&mut self) -> Result<bool, BackhandError> {
        let Some(step) = self.steps.pop_front() else {
            return Ok(false);
        };
        match step {
            Step::EmptyFile { file } => {
                let blocks_start = self.writer.stream_position()?;
                self.files[file] =
                    Some((0, Added::Data { blocks_start, block_sizes: vec![], sparse: 0 }));
            }
            Step::FileStart => {
                let blocks_start = self.writer.stream_position()?;
                self.current_file =
                    Some(BlockFile { blocks_start, block_sizes: vec![], sparse: 0 });
            }
            Step::Block(block) => {
                let size = self.write_block(block)?;
                self.current_file()?.block_sizes.push(size);
            }
            Step::Hole { len } => {
                let current_file = self.current_file()?;
                current_file.block_sizes.push(DataSize::hole());
                current_file.sparse += len;
            }
            Step::FileEnd { file, file_len, hash } => {
                let BlockFile { blocks_start, block_sizes, sparse } =
                    self.current_file.take().ok_or(BackhandError::InternalState(err_text!(
                        "file end without file start"
                    )))?;
                let added = (file_len, Added::Data { blocks_start, block_sizes, sparse });
                self.files[file] = Some(match &mut self.data.dup_cache {
                    None => added,
                    // The hash is known only after the whole file is read. Thus, like
                    // mksquashfs, write the data first, then seek back so that the next data
                    // writes over a duplicate.
                    Some(dup_cache) => {
                        let files_with_len = dup_cache.entry(file_len as u64).or_default();
                        match files_with_len.entry(hash) {
                            Entry::Occupied(original) => {
                                trace!("duplicate file data found");
                                self.writer.seek(SeekFrom::Start(blocks_start))?;
                                original.get().clone()
                            }
                            Entry::Vacant(entry) => entry.insert(added).clone(),
                        }
                    }
                });
            }
            Step::Fragment(block) => {
                let start = self.writer.stream_position()?;
                let size = self.write_block(block)?;
                self.data.fragment_table.push(Fragment::new(start, size, 0));
            }
        }
        Ok(true)
    }

    fn current_file(&mut self) -> Result<&mut BlockFile, BackhandError> {
        self.current_file
            .as_mut()
            .ok_or(BackhandError::InternalState(err_text!("block without file start")))
    }

    /// Writes `block` once it is compressed, uncompressed if compression did not reduce its size
    fn write_block(&mut self, block: Block) -> Result<DataSize, BackhandError> {
        let CompressedBlock { raw, compressed } = block.wait()?;
        self.compressing -= 1;
        let compressed = compressed?;
        let raw_bytes = raw.bytes();
        // compression didn't reduce size
        let size = if compressed.len() > raw_bytes.len() {
            // store uncompressed
            self.writer.write_all(raw_bytes)?;
            DataSize::new_uncompressed(raw_bytes.len() as u32)
        } else {
            // store compressed
            self.writer.write_all(&compressed)?;
            DataSize::new_compressed(compressed.len() as u32)
        };
        self.free_buffers.push(raw.buffer);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{
        DEFAULT_BLOCK_SIZE,
        compression::{Compressor, DefaultCompressor},
    };

    #[cfg(feature = "gzip")]
    fn gzip_data_writer(no_duplicate_files: bool) -> DataWriter<'static> {
        DataWriter::new(
            &DefaultCompressor,
            FilesystemCompressor::new(Compressor::Gzip, None).unwrap(),
            DEFAULT_BLOCK_SIZE,
            no_duplicate_files,
        )
    }

    /// Bytes that do not compress, from a linear congruential generator
    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 56) as u8
            })
            .collect()
    }

    /// Files of every kind the data writer stores differently
    fn files() -> Vec<Vec<u8>> {
        let block = DEFAULT_BLOCK_SIZE as usize;
        let mut sparse = vec![0u8; block * 3];
        sparse[block * 2..].copy_from_slice(&noise(block, 1));
        let mut files = vec![
            vec![],
            noise(block * 5 + 17, 2),
            vec![b'a'; block * 4],
            sparse,
            noise(block * 5 + 17, 2),
        ];
        for index in 0..300 {
            files.push(noise(1000 + index * 37, index as u64));
            files.push(vec![index as u8; 500]);
        }
        files.push(noise(block, 3));
        files
    }

    #[cfg(feature = "gzip")]
    fn write(threads: usize, files: &[Vec<u8>]) -> (Vec<u8>, Vec<(usize, Added)>, Vec<Fragment>) {
        let mut data_writer = gzip_data_writer(true);
        let mut writer = Cursor::new(vec![]);
        let added = data_writer
            .write_files(NonZeroUsize::new(threads).unwrap(), &mut writer, |session| {
                for file in files {
                    session.add_bytes(&file[..])?;
                }
                Ok(())
            })
            .unwrap();
        data_writer.finalize(&mut writer).unwrap();
        (writer.into_inner(), added, data_writer.fragment_table)
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_duplicate_check() {
        let bytes = [0xff_u8; DEFAULT_BLOCK_SIZE as usize * 2];
        let mut data_writer = gzip_data_writer(true);
        let mut writer = Cursor::new(vec![]);
        let added = data_writer
            .write_files(NonZeroUsize::MIN, &mut writer, |session| {
                session.add_bytes(&bytes[..])?;
                session.add_bytes(&bytes[..])
            })
            .unwrap();
        assert_eq!(added[0], added[1]);
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_no_duplicate_check() {
        let bytes = [0xff_u8; DEFAULT_BLOCK_SIZE as usize * 2];
        let mut data_writer = gzip_data_writer(false);
        let mut writer = Cursor::new(vec![]);
        let added = data_writer
            .write_files(NonZeroUsize::MIN, &mut writer, |session| {
                session.add_bytes(&bytes[..])?;
                session.add_bytes(&bytes[..])
            })
            .unwrap();
        assert_ne!(added[0], added[1]);
    }

    /// Panics when it compresses the block of a file of `0xff` bytes
    struct PanickingCompressor;

    impl crate::traits::CompressionAction for PanickingCompressor {
        type Error = BackhandError;
        type Compressor = Compressor;
        type FilesystemCompressor = FilesystemCompressor;
        type SuperBlock = crate::v4::squashfs::SuperBlock;

        fn decompress(
            &self,
            _: &[u8],
            _: &mut Vec<u8>,
            _: Compressor,
        ) -> Result<(), BackhandError> {
            unreachable!()
        }

        fn compress(
            &self,
            bytes: &[u8],
            _: FilesystemCompressor,
            _: u32,
        ) -> Result<Vec<u8>, BackhandError> {
            assert!(bytes[0] != 0xff, "compression failed");
            Ok(bytes.to_vec())
        }
    }

    #[test]
    fn test_panicking_compression_thread_fails_the_write() {
        let mut files = files();
        files.insert(40, vec![0xff; DEFAULT_BLOCK_SIZE as usize]);
        let mut data_writer = DataWriter::new(
            &PanickingCompressor,
            FilesystemCompressor::default(),
            DEFAULT_BLOCK_SIZE,
            true,
        );
        let mut writer = Cursor::new(vec![]);
        // The panic of the compression thread surfaces once the write has ended, instead of the
        // write waiting for the block forever
        let write = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            data_writer.write_files(NonZeroUsize::new(4).unwrap(), &mut writer, |session| {
                for file in &files {
                    session.add_bytes(&file[..])?;
                }
                Ok(())
            })
        }));
        assert!(write.is_err());
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_threads_write_the_same_data() {
        let files = files();
        let sequential = write(1, &files);
        for threads in [2, 8] {
            assert!(write(threads, &files) == sequential, "{threads} threads wrote other data");
        }
        // Holes, duplicates, fragments and incompressible blocks are all in the data
        let (_, added, fragments) = sequential;
        assert!(
            added
                .iter()
                .any(|(_, added)| matches!(added, Added::Data { sparse, .. } if *sparse > 0))
        );
        assert_eq!(added[1], added[4]);
        assert!(fragments.len() > 1);
        assert!(added.iter().any(|(_, added)| matches!(added, Added::Data { block_sizes, .. }
            if block_sizes.iter().any(DataSize::uncompressed))));
    }
}
