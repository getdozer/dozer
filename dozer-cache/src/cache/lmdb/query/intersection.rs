use roaring::{MultiOps, RoaringTreemap};

pub fn intersection<I: Iterator<Item = u64>>(
    iterators: impl Iterator<Item = I>,
) -> impl Iterator<Item = u64> {
    let treemaps = iterators.map(|iter| RoaringTreemap::from_iter(iter));
    treemaps.intersection().into_iter()
}
