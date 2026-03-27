use {agave_scheduler_bindings::SharableTransactionRegion, rts_alloc::Allocator};

/// Allocate shared memory and copy raw transaction bytes into it.
pub fn allocate_transaction(
    data: &[u8],
    allocator: &Allocator,
) -> Option<SharableTransactionRegion> {
    let ptr = allocator.allocate(data.len() as u32)?;
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        Some(SharableTransactionRegion {
            offset: allocator.offset(ptr),
            length: data.len() as u32,
        })
    }
}
