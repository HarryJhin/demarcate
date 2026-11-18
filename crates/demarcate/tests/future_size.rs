#[path = "../../demarcate-interceptor/tests/support/mod.rs"]
mod support;

use demarcate::transactional;
use support::AppError;

#[transactional(transaction_manager = "size")]
async fn leaf() -> Result<(), AppError> {
    Ok(())
}

#[transactional(transaction_manager = "size")]
async fn parent() -> Result<(), AppError> {
    leaf().await?;
    leaf().await
}

#[transactional(transaction_manager = "size")]
async fn grandparent() -> Result<(), AppError> {
    parent().await?;
    parent().await
}

#[test]
fn transactional_future_size_does_not_grow_with_nesting() {
    let leaf = std::mem::size_of_val(&leaf());
    let parent = std::mem::size_of_val(&parent());
    let grandparent = std::mem::size_of_val(&grandparent());
    assert_eq!(leaf, parent);
    assert_eq!(parent, grandparent);
}
