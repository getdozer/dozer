use roaring::{MultiOps, RoaringTreemap};

pub fn intersection<I: Iterator<Item = u64>>(
    iterators: impl Iterator<Item = I>,
) -> impl Iterator<Item = u64> {
    let treemaps = iterators.map(|iter| RoaringTreemap::from_iter(iter));
    treemaps.intersection().into_iter()
}

#[test]
fn test_intersection() {
    let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let b = vec![1, 3, 5, 7, 9];
    let c = vec![1, 2, 3, 5, 8];
    let intersection = intersection([a.into_iter(), b.into_iter(), c.into_iter()].into_iter());
    assert_eq!(intersection.collect::<Vec<_>>(), vec![1, 3, 5]);
}
