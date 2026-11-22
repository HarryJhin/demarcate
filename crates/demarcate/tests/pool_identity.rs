use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use demarcate::postgres::TransactionalPool;
use sqlx::postgres::PgPoolOptions;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

// Count only allocations made by the current thread during the synchronous clone below.
// All memory operations still use the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[tokio::test]
async fn cloning_a_transactional_pool_does_not_allocate() {
    let raw = PgPoolOptions::new().connect_lazy("postgres://localhost/unused").unwrap();
    let pool = TransactionalPool::new(raw);

    ALLOCATIONS.with(|count| count.set(Some(0)));
    let cloned = black_box(pool.clone());
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());

    assert_eq!(allocations, 0, "cloning a transactional pool must not allocate a resource key");
    assert_eq!(cloned.resource_key(), pool.resource_key());
}

#[tokio::test]
async fn clones_keep_their_identity_while_independent_wrappers_stay_distinct() {
    let raw = PgPoolOptions::new().connect_lazy("postgres://localhost/unused").unwrap();
    let pool = TransactionalPool::new(raw.clone());
    let cloned = pool.clone();
    let original_key = pool.resource_key().clone();
    drop(pool);

    assert_eq!(cloned.resource_key(), &original_key);
    for _ in 0..1024 {
        let independent = TransactionalPool::new(raw.clone());
        assert_ne!(independent.resource_key(), cloned.resource_key());
        assert_eq!(independent.resource_key(), independent.clone().resource_key());
    }
}
