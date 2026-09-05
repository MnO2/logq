//! Controlled JSON scanner probe; allocation counting is a separate measurement.
use logq::bench_internals::{TypedColumn, json_batch_scanner, json_like_filter};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

struct CountingAllocator;
static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_pointer
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 4 {
        return Err("usage: json_scan_probe PATH buffered8k|buffered64k|buffered1m|mapped on|off FIELD,...|- [--like PATTERN] [--allocations]".into());
    }
    let mut allocations = false;
    let mut pattern = None;
    let mut index = 4;
    while index < args.len() {
        match args[index].as_str() {
            "--allocations" => allocations = true,
            "--like" if index + 1 < args.len() => {
                index += 1;
                pattern = Some(args[index].as_str());
            }
            _ => return Err("unknown or incomplete probe option".into()),
        }
        index += 1;
    }
    let dictionary = match args[2].as_str() {
        "on" => true,
        "off" => false,
        _ => return Err("dictionary must be on or off".into()),
    };
    let fields: Vec<String> = if args[3] == "-" {
        Vec::new()
    } else {
        args[3].split(',').map(str::to_owned).collect()
    };
    let filter_field = fields.first().cloned();
    if pattern.is_some() && filter_field.is_none() {
        return Err("LIKE requires a selected field".into());
    }
    let file = File::open(&args[0])?;
    let input_bytes = file.metadata()?.len();
    let reader: Box<dyn BufRead> = match args[1].as_str() {
        "buffered8k" => Box::new(BufReader::with_capacity(8 * 1024, file)),
        "buffered64k" => Box::new(BufReader::with_capacity(64 * 1024, file)),
        "buffered1m" => Box::new(BufReader::with_capacity(1024 * 1024, file)),
        "mapped" => {
            // This diagnostic requires an immutable input for the scan lifetime.
            let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
            Box::new(Cursor::new(map))
        }
        _ => return Err("unknown reader backend".into()),
    };
    ENABLED.store(allocations, Ordering::Relaxed);
    let start = Instant::now();
    let mut scanner = json_batch_scanner(reader, fields, dictionary);
    if let Some(pattern) = pattern {
        scanner = json_like_filter(scanner, filter_field.as_deref().unwrap(), pattern);
    }
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut active_rows = 0u64;
    let mut dictionary_columns = 0u64;
    while let Some(batch) = scanner.next_batch()? {
        rows += batch.len as u64;
        active_rows += batch.selection.count_active(batch.len) as u64;
        batches += 1;
        dictionary_columns += batch
            .columns
            .iter()
            .filter(|c| matches!(c, TypedColumn::DictUtf8 { .. }))
            .count() as u64;
        std::hint::black_box(batch);
    }
    drop(scanner);
    let elapsed_ns = start.elapsed().as_nanos();
    ENABLED.store(false, Ordering::Relaxed);
    println!(
        "{}",
        serde_json::json!({
            "input_bytes": input_bytes, "rows": rows, "batches": batches,
            "active_rows": active_rows, "like_pattern": pattern,
            "backend": args[1], "dictionary": dictionary,
            "dictionary_columns": dictionary_columns, "elapsed_ns": elapsed_ns,
            "allocation_instrumentation": allocations,
            "allocation_calls": allocations.then(|| CALLS.load(Ordering::Relaxed)),
            "allocated_bytes_including_realloc": allocations.then(|| BYTES.load(Ordering::Relaxed)),
        })
    );
    Ok(())
}
