//! Reading a file's data, shared by all SquashFS versions

use no_std_io2::io::Read;
use std::collections::VecDeque;
use std::io::{Seek, SeekFrom};
use std::sync::mpsc;

use crate::error::BackhandError;
use crate::traits::block_reader::{
    BlockFragment, BlockIterator, BlockReaderVersion, RawDataBlock, cache_fragment, decompress,
    read_raw_data,
};
use crate::traits::decompression::DecompressedBlock;

/// Reads a file's data blocks from the image, without decompressing them
pub struct SquashfsRawData<'a, 'b, V: BlockReaderVersion<'b>> {
    pub(crate) system: &'a V::System,
    pub(crate) file: &'a V::File,
    current_block: BlockIterator<'a, 'b, V>,
    /// Offset in the image of the next whole block to read
    pub(crate) pos: u64,
}

impl<'a, 'b, V: BlockReaderVersion<'b>> SquashfsRawData<'a, 'b, V> {
    pub fn new(system: &'a V::System, file: &'a V::File) -> Result<Self, BackhandError> {
        let pos = V::blocks_start(file);
        let current_block =
            BlockIterator { blocks: V::block_sizes(file), fragment: V::fragment_of(system, file)? };
        Ok(Self { system, file, current_block, pos })
    }

    /// Like [`Self::new`], but reads only the file's whole blocks, ignoring its fragment
    pub fn new_without_fragment(system: &'a V::System, file: &'a V::File) -> Self {
        let pos = V::blocks_start(file);
        let current_block = BlockIterator { blocks: V::block_sizes(file), fragment: None };
        Self { system, file, current_block, pos }
    }

    #[inline]
    pub fn next_block(&mut self, buf: &mut Vec<u8>) -> Option<Result<RawDataBlock, BackhandError>> {
        self.current_block
            .next()
            .map(|next| read_raw_data::<V>(self.system, self.file, &mut self.pos, buf, &next))
    }

    // Advance position by one block without reading/decompressing - internal to Seek impl
    #[inline]
    pub(crate) fn skip_block(&mut self) -> bool {
        match self.current_block.next() {
            Some(BlockFragment::Block(block)) => {
                // correctly adds 0 for sparse blocks (size == 0)
                self.pos += u64::from(V::data_size(block));
                true
            }
            Some(BlockFragment::Fragment(_)) => true, // fragment is last, just consume it
            None => false,
        }
    }

    /// Decompress `data` on the calling thread
    pub fn decompress(
        &self,
        data: RawDataBlock,
        input_buf: &mut Vec<u8>,
        output_buf: &mut Vec<u8>,
    ) -> Result<(), BackhandError> {
        decompress::<V>(self.system, self.file, data, input_buf, output_buf)
    }

    #[inline]
    pub fn into_reader(self) -> SquashfsReadFile<'a, 'b, V> {
        SquashfsReadFile::new(self)
    }
}

/// A block of a file on its way to the reader
enum Block {
    Decompressed(Vec<u8>),
    /// A decompression thread sends the block once it is decompressed, or drops the sender if it
    /// panicked
    Decompressing {
        fragment: bool,
        result: mpsc::Receiver<DecompressedBlock>,
    },
}

/// A [`Read`] + [`Seek`] handle over one file's decompressed contents
///
/// The reader keeps as many blocks in flight as the filesystem has decompression threads.
pub struct SquashfsReadFile<'a, 'b, V: BlockReaderVersion<'b>> {
    raw_data: SquashfsRawData<'a, 'b, V>,
    /// The file's next blocks, in order. The front block, if any, is decompressed and holds the
    /// byte at `cursor_pos`, unless the reader is at its end.
    blocks: VecDeque<Block>,
    /// Offset of `cursor_pos` in the front block
    current_block_position: usize,
    cursor_pos: u64,
    /// Buffers for reuse
    buffers: Vec<Vec<u8>>,
}

impl<'a, 'b, V: BlockReaderVersion<'b>> SquashfsReadFile<'a, 'b, V> {
    fn new(raw_data: SquashfsRawData<'a, 'b, V>) -> Self {
        Self {
            raw_data,
            blocks: VecDeque::new(),
            current_block_position: 0,
            cursor_pos: 0,
            buffers: Vec::new(),
        }
    }

    #[inline]
    fn file_len64(&self) -> u64 {
        V::file_len(self.raw_data.file) as u64
    }

    /// The bytes of the front block from `cursor_pos` on
    #[inline]
    fn available(&self) -> &[u8] {
        match self.blocks.front() {
            Some(Block::Decompressed(block)) => &block[self.current_block_position..],
            _ => &[],
        }
    }

    #[inline]
    fn read_available(&mut self, buf: &mut [u8]) -> usize {
        let available = self.available();
        let bytes_left = self.file_len64().saturating_sub(self.cursor_pos);
        let read_len = bytes_left.min(buf.len().min(available.len()) as u64) as usize;
        buf[..read_len].copy_from_slice(&available[..read_len]);
        self.cursor_pos += read_len as u64;
        self.current_block_position += read_len;
        read_len
    }

    /// Moves on to the next block once the front block is read, and starts decompressing the
    /// blocks after it
    fn fill(&mut self) -> Result<(), BackhandError> {
        if !self.blocks.is_empty() && self.available().is_empty() {
            if let Some(Block::Decompressed(block)) = self.blocks.pop_front() {
                self.buffers.push(block);
            }
            self.current_block_position = 0;
        }
        self.read_ahead()?;
        self.wait_for_front()
    }

    /// Reads blocks from the image until as many are in flight as the filesystem has
    /// decompression threads, and starts decompressing them
    fn read_ahead(&mut self) -> Result<(), BackhandError> {
        let decompression = V::decompression(self.raw_data.system);
        while self.blocks.len() < decompression.threads().get() {
            let mut raw = self.buffers.pop().unwrap_or_default();
            let Some(block) = self.raw_data.next_block(&mut raw) else {
                self.buffers.push(raw);
                return Ok(());
            };
            let block = block?;
            let mut decompressed = self.buffers.pop().unwrap_or_default();
            decompressed.clear();
            match decompression.workers() {
                Some(workers) if !block.uncompressed => {
                    let result = workers.decompress(raw, decompressed)?;
                    self.blocks
                        .push_back(Block::Decompressing { fragment: block.fragment, result });
                }
                _ => {
                    self.raw_data.decompress(block, &mut raw, &mut decompressed)?;
                    self.buffers.push(raw);
                    self.blocks.push_back(Block::Decompressed(decompressed));
                }
            }
        }
        Ok(())
    }

    /// Waits until the front block is decompressed
    fn wait_for_front(&mut self) -> Result<(), BackhandError> {
        let Some(front) = self.blocks.front_mut() else { return Ok(()) };
        let Block::Decompressing { fragment, result } = front else { return Ok(()) };
        let DecompressedBlock { compressed, decompressed } = result.recv().map_err(|_| {
            BackhandError::InternalState(err_text!("a decompression thread panicked"))
        })?;
        let fragment = *fragment;
        self.buffers.push(compressed);
        let mut decompressed = decompressed?;
        if fragment {
            cache_fragment::<V>(self.raw_data.system, self.raw_data.file, &mut decompressed)?;
        }
        *front = Block::Decompressed(decompressed);
        Ok(())
    }

    /// Drops the blocks in flight, keeping the buffers of those that are decompressed
    fn clear_blocks(&mut self) {
        for block in self.blocks.drain(..) {
            if let Block::Decompressed(buffer) = block {
                self.buffers.push(buffer);
            }
        }
        self.current_block_position = 0;
    }
}

impl<'b, V: BlockReaderVersion<'b>> Read for SquashfsReadFile<'_, 'b, V> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // file was fully consumed
        if self.cursor_pos >= self.file_len64() {
            return Ok(0);
        }
        //no data available, read the next block
        if self.available().is_empty() {
            self.fill()?;
        }

        //return data from the read block/fragment
        Ok(self.read_available(buf))
    }
}

impl<'b, V: BlockReaderVersion<'b>> Seek for SquashfsReadFile<'_, 'b, V> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let file_len = self.file_len64();
        let new_pos = u64::try_from(match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => file_len as i64 + n,
            SeekFrom::Current(n) => self.cursor_pos as i64 + n,
        })
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;

        if new_pos == self.cursor_pos {
            return Ok(new_pos);
        }

        // can we seek within already-loaded data, inclusive of end positions?
        if let Some(Block::Decompressed(block)) = self.blocks.front() {
            let block_start = self.cursor_pos.min(file_len) - self.current_block_position as u64;
            if new_pos >= block_start && new_pos - block_start <= block.len() as u64 {
                // seek within already-loaded data
                self.current_block_position = (new_pos - block_start) as usize;
                self.cursor_pos = new_pos;
                return Ok(new_pos);
            }
        }

        // reset to initial start-of-file state and then skip forward
        self.raw_data = SquashfsRawData::new(self.raw_data.system, self.raw_data.file)
            .map_err(std::io::Error::other)?;
        self.clear_blocks();
        self.cursor_pos = 0;

        if new_pos < file_len {
            // skip full blocks without decompressing them
            let block_size = u64::from(V::block_size(self.raw_data.system));
            while new_pos >= self.cursor_pos + block_size {
                let _skipped = self.raw_data.skip_block();
                debug_assert!(_skipped);
                self.cursor_pos += block_size;
            }
            // no block or fragment loaded yet - load now if necessary, else wait for read
            if new_pos != self.cursor_pos {
                self.fill()?;
                debug_assert!(new_pos <= self.cursor_pos + self.available().len() as u64);
                self.current_block_position = (new_pos - self.cursor_pos) as usize;
                self.cursor_pos = new_pos;
            }
        } else {
            // drain block iterator to ensure consistent end-of-file state
            while self.raw_data.skip_block() {}
            self.cursor_pos = new_pos; // note, may be greater than file_len
        }

        Ok(new_pos)
    }
}

#[cfg(test)]
#[cfg(feature = "gzip")]
mod tests {
    use std::io::{Cursor, Read, Seek, SeekFrom};
    use std::num::NonZeroUsize;
    use std::path::Path;

    use crate::compression::{Compressor, DefaultCompressor};
    use crate::kinds::Kind;
    use crate::v4::filesystem::writer::FilesystemCompressor;
    use crate::{
        BackhandError, DEFAULT_BLOCK_SIZE, FilesystemReader, FilesystemWriter, InnerNode,
        NodeHeader,
    };

    const BLOCK: usize = DEFAULT_BLOCK_SIZE as usize;

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

    /// Bytes that compress, and differ in every block
    fn text(len: usize) -> Vec<u8> {
        (0..).flat_map(|line: u32| format!("line {line}\n").into_bytes()).take(len).collect()
    }

    /// Files of every kind the reader reads differently
    fn files() -> Vec<Vec<u8>> {
        let mut sparse = vec![0u8; BLOCK * 3];
        sparse[BLOCK * 2..].copy_from_slice(&text(BLOCK));
        let mut files = vec![vec![], text(BLOCK * 20 + 17), noise(BLOCK * 3 + 5, 1), sparse];
        for index in 0..50 {
            files.push(text(1000 + index * 37));
        }
        files
    }

    /// An image with the file `/<index>` for each of `files`
    fn image(files: &[Vec<u8>]) -> Vec<u8> {
        let mut writer = FilesystemWriter::new(NonZeroUsize::MIN);
        writer.set_compressor(FilesystemCompressor::new(Compressor::Gzip, None).unwrap());
        for (index, file) in files.iter().enumerate() {
            writer.push_file(Cursor::new(file), index.to_string(), NodeHeader::default()).unwrap();
        }
        let mut image = Cursor::new(vec![]);
        writer.write(&mut image).unwrap();
        image.into_inner()
    }

    fn file_reader<'a, 'b>(
        filesystem: &'a FilesystemReader<'b>,
        index: usize,
    ) -> crate::v4::filesystem::reader::SquashfsReadFile<'a, 'b> {
        let path = Path::new("/").join(index.to_string());
        let node = filesystem.files().find(|node| node.fullpath == path).unwrap();
        let InnerNode::File(file) = &node.inner else { panic!("{} is no file", path.display()) };
        filesystem.file(file).reader()
    }

    fn threads(threads: usize) -> NonZeroUsize {
        NonZeroUsize::new(threads).unwrap()
    }

    #[test]
    fn test_threads_read_the_same_data() {
        // Arrange
        let files = files();
        let image = image(&files);
        for threads in [threads(1), threads(2), threads(8)] {
            let filesystem = FilesystemReader::from_reader(Cursor::new(&image), threads).unwrap();
            for (index, expected) in files.iter().enumerate() {
                // Act
                let mut content = vec![];
                file_reader(&filesystem, index).read_to_end(&mut content).unwrap();

                // Assert
                assert!(content == *expected, "{threads} threads read other data for file {index}");
            }
        }
    }

    #[test]
    fn test_seek_with_blocks_in_flight() {
        // Arrange
        let files = files();
        let image = image(&files);
        let expected = &files[1];
        let len = expected.len() as u64;
        let filesystem = FilesystemReader::from_reader(Cursor::new(&image), threads(8)).unwrap();
        let mut reader = file_reader(&filesystem, 1);
        let positions = [
            SeekFrom::Start(10),
            SeekFrom::Current(100),
            SeekFrom::Start(BLOCK as u64 * 5 - 50),
            SeekFrom::Start(3),
            SeekFrom::Current(BLOCK as u64 as i64 * 12),
            SeekFrom::End(-200),
            SeekFrom::Start(BLOCK as u64 * 15),
        ];
        for position in positions {
            // Act
            let start = reader.seek(position).unwrap();
            let mut content = vec![0; 300];
            let read = reader.read(&mut content).unwrap();

            // Assert
            let start = start as usize;
            let end = (start + read).min(expected.len());
            assert!(read > 0, "read nothing at {start}");
            assert!(content[..read] == expected[start..end], "read other data at {start}");
        }
        assert_eq!(reader.seek(SeekFrom::End(0)).unwrap(), len);
        assert_eq!(reader.read(&mut [0; 10]).unwrap(), 0);
    }

    #[test]
    fn test_dropping_a_reader_with_blocks_in_flight() {
        // Arrange
        let files = files();
        let image = image(&files);
        let filesystem = FilesystemReader::from_reader(Cursor::new(&image), threads(4)).unwrap();

        // Act
        file_reader(&filesystem, 1).read_exact(&mut [0; 10]).unwrap();
        let mut content = vec![];
        file_reader(&filesystem, 1).read_to_end(&mut content).unwrap();

        // Assert
        assert!(content == files[1]);
        // Dropping the filesystem ends its threads
        drop(filesystem);
    }

    /// Panics when it decompresses a block that starts with the bytes of `line 5000`
    struct PanickingDecompressor;

    impl crate::traits::CompressionAction for PanickingDecompressor {
        type Error = BackhandError;
        type Compressor = Compressor;
        type FilesystemCompressor = FilesystemCompressor;
        type SuperBlock = crate::v4::squashfs::SuperBlock;

        fn decompress(
            &self,
            bytes: &[u8],
            out: &mut Vec<u8>,
            compressor: Compressor,
        ) -> Result<(), BackhandError> {
            DefaultCompressor.decompress(bytes, out, compressor)?;
            assert!(!out.starts_with(b"line 5000\n"), "decompression failed");
            Ok(())
        }

        fn compress(
            &self,
            bytes: &[u8],
            fc: FilesystemCompressor,
            block_size: u32,
        ) -> Result<Vec<u8>, BackhandError> {
            DefaultCompressor.compress(bytes, fc, block_size)
        }
    }

    #[test]
    fn test_panicking_decompression_thread_fails_the_read() {
        // Arrange
        let mut file = text(BLOCK * 20);
        let start = file.windows(10).position(|line| line == b"line 5000\n").unwrap();
        file.drain(..start);
        let image = image(&[file]);
        let kind = Kind::new_v4(&PanickingDecompressor);
        let filesystem = FilesystemReader::from_reader_with_offset_and_kind(
            Cursor::new(&image),
            0,
            kind,
            threads(4),
        )
        .unwrap();

        // Act
        let read = file_reader(&filesystem, 0).read_to_end(&mut vec![]);

        // Assert
        assert!(read.is_err());
    }
}
