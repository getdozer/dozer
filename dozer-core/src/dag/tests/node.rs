use crate::dag::node::NodeHandle;

#[test]
fn test_handle_to_from_bytes() {
    let original = NodeHandle::new(Some(10), 100.to_string());
    let sz = original.to_bytes();
    let _decoded = NodeHandle::from_bytes(sz.as_slice());

    let original = NodeHandle::new(None, 100.to_string());
    let sz = original.to_bytes();
    let decoded = NodeHandle::from_bytes(sz.as_slice());

    assert_eq!(original, decoded)
}
