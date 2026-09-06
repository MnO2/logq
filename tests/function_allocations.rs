use logq::common::types::Value;
use logq::functions::register_all;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct TrackingAllocator;

thread_local! {
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}

fn track(bytes: usize) {
    let _ = ALLOCATED.try_with(|allocated| {
        if let Some(total) = allocated.get() {
            allocated.set(Some(total.saturating_add(bytes)));
        }
    });
}

// Forward allocation unchanged, recording only the current test thread.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        track(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        track(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn allocated_during(f: impl FnOnce()) -> usize {
    ALLOCATED.with(|allocated| allocated.set(Some(0)));
    f();
    ALLOCATED.with(|allocated| allocated.replace(None).unwrap())
}

#[test]
fn levenshtein_workspace_does_not_grow_quadratically() {
    let registry = register_all().unwrap();
    let args = [
        Value::String("a".repeat(512).into()),
        Value::String("b".repeat(512).into()),
    ];
    let allocated = allocated_during(|| {
        assert_eq!(registry.call("levenshtein_distance", &args), Ok(Value::Int(512)));
    });
    eprintln!("512 x 512 edit distance allocated {allocated} bytes");
    assert!(
        allocated < 64 * 1024,
        "512-character edit distance allocated {allocated} bytes"
    );

    let args = [Value::String("abc".into()), Value::String("a".repeat(32768).into())];
    let allocated = allocated_during(|| {
        assert_eq!(registry.call("levenshtein_distance", &args), Ok(Value::Int(32767)));
    });
    eprintln!("3 x 32768 edit distance allocated {allocated} bytes");
    assert!(allocated < 4096, "asymmetric edit distance allocated {allocated} bytes");
}

#[test]
fn split_part_does_not_collect_unrequested_segments() {
    let registry = register_all().unwrap();
    let args = [
        Value::String("a/".repeat(100_000).into()),
        Value::String("/".into()),
        Value::Int(1),
    ];
    let allocated = allocated_during(|| {
        assert_eq!(registry.call("split_part", &args), Ok(Value::String("a".into())));
    });
    eprintln!("first of 100,000 split segments allocated {allocated} bytes");
    assert!(allocated < 4096, "split_part allocated {allocated} bytes");
}

#[test]
fn cached_regex_reuses_matching_workspace() {
    let registry = register_all().unwrap();
    let args = [
        Value::String("request-123456-complete".into()),
        Value::String(r"[a-z]+-\d+-[a-z]+".into()),
    ];
    assert_eq!(registry.call("regexp_like", &args), Ok(Value::Boolean(true)));
    let allocated = allocated_during(|| {
        for _ in 0..1000 {
            assert_eq!(registry.call("regexp_like", &args), Ok(Value::Boolean(true)));
        }
    });
    eprintln!("1,000 cached regex matches allocated {allocated} bytes");
    assert!(allocated < 4096, "cached regex matching allocated {allocated} bytes");
}
