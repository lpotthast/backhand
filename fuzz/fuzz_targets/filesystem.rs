#![no_main]

use std::num::NonZeroUsize;

use backhand::FilesystemReader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let reader = std::io::Cursor::new(data);
    let _ = FilesystemReader::from_reader(reader, NonZeroUsize::MIN);
});
