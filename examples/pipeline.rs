//! A minimal pipeline stage: read stdin through a `ReadAhead`, do some
//! per-byte "compute" (here: ASCII-uppercasing), and write stdout through a
//! `WriteBehind` — so the stage's compute overlaps both the reads and the
//! writes instead of blocking on the kernel pipe at either end.
//!
//! Try it:
//!
//! ```sh
//! printf 'hello, pipeline\n' | cargo run --example pipeline
//! ```

use std::io::{self, BufRead, Write};

use rawb_io::{ReadAhead, WriteBehind};

fn main() -> io::Result<()> {
    // 8 MiB of read-ahead and write-behind either side of the transform. The
    // thread-name prefixes label the IO threads (and any surfaced IO-thread
    // panic) after this stage. Note `Stdin`/`Stdout` (Send), not their lock
    // guards: the sources move onto the IO threads.
    let mut reader = ReadAhead::with_thread_name(io::stdin(), 8 << 20, "pipeline-src");
    let mut writer = WriteBehind::with_thread_name(io::stdout(), 8 << 20, "pipeline-dst");

    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            break; // end of stream
        }
        // The stand-in for real per-byte work.
        let transformed: Vec<u8> = chunk.iter().map(u8::to_ascii_uppercase).collect();
        let consumed = chunk.len();
        writer.write_all(&transformed)?;
        reader.consume(consumed);
    }

    // Drain the ring, flush stdout, join the IO thread, surface any error.
    writer.finish()?;
    Ok(())
}
