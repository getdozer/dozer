use roaring::{MultiOps, RoaringTreemap};

pub fn intersection<I: Iterator<Item = u64>>(
    iterators: Vec<I>,
    chunk_size: usize,
) -> Intersetion<I> {
    let all_iterated_ids = vec![RoaringTreemap::new(); iterators.len()];
    Intersetion {
        intersection: None,
        iterators,
        all_iterated_ids,
        chunk_size,
    }
}

pub struct Intersetion<I: Iterator<Item = u64>> {
    intersection: Option<roaring::treemap::IntoIter>,
    iterators: Vec<I>,
    all_iterated_ids: Vec<RoaringTreemap>,
    chunk_size: usize,
}

impl<I: Iterator<Item = u64>> Iterator for Intersetion<I> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(intersection) = &mut self.intersection {
                if let Some(id) = intersection.next() {
                    return Some(id);
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
                        iterated_ids.insert(id);
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

#[test]
fn test_intersection() {
    let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let b = vec![1, 3, 5, 7, 9];
    let c = vec![1, 2, 3, 5, 8];
    let intersection = intersection(vec![a.into_iter(), b.into_iter(), c.into_iter()], 2);
    assert_eq!(intersection.collect::<Vec<_>>(), vec![1, 3, 5]);
}
