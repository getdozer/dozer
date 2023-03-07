use roaring::{MultiOps, RoaringTreemap};

pub fn intersection<E, I: Iterator<Item = Result<u64, E>>>(
    iterators: Vec<I>,
    chunk_size: usize,
) -> Intersection<E, I> {
    let all_iterated_ids = vec![RoaringTreemap::new(); iterators.len()];
    Intersection {
        intersection: None,
        iterators,
        all_iterated_ids,
        chunk_size,
    }
}

pub struct Intersection<E, I: Iterator<Item = Result<u64, E>>> {
    intersection: Option<roaring::treemap::IntoIter>,
    iterators: Vec<I>,
    all_iterated_ids: Vec<RoaringTreemap>,
    chunk_size: usize,
}

impl<E, I: Iterator<Item = Result<u64, E>>> Iterator for Intersection<E, I> {
    type Item = Result<u64, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(intersection) = &mut self.intersection {
                if let Some(id) = intersection.next() {
                    return Some(Ok(id));
                } else {
                    self.intersection = None;
                }
            }

            let mut exhaused = true;

            // Get the next chunk of each iterator.
            for (iterated_ids, iterator) in self
                .all_iterated_ids
                .iter_mut()
                .zip(self.iterators.iter_mut())
            {
                for _ in 0..self.chunk_size {
                    if let Some(id) = iterator.next() {
                        exhaused = false;
                        match id {
                            Ok(id) => {
                                iterated_ids.insert(id);
                            }
                            Err(e) => {
                                return Some(Err(e));
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            if exhaused {
                return None;
            }

            // Emit the intersection of all ids.
            let intersection = self.all_iterated_ids.iter().intersection();
            for iterated_ids in self.all_iterated_ids.iter_mut() {
                *iterated_ids -= &intersection;
            }
            self.intersection = Some(intersection.into_iter());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[test]
    fn test_intersection() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let b = vec![1, 3, 5, 7, 9];
        let c = vec![1, 2, 3, 5, 8];
        let intersection = intersection(
            vec![
                a.into_iter().map(Ok),
                b.into_iter().map(Ok),
                c.into_iter().map(Ok),
            ],
            2,
        );
        assert_eq!(
            intersection
                .collect::<Result<Vec<_>, Infallible>>()
                .unwrap(),
            vec![1, 3, 5]
        );
    }
}
