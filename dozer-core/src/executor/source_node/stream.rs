use async_stream::stream;
use dozer_log::tokio::sync::mpsc::Receiver;
use futures::{future::select_all, Stream};

/// A convenient way of getting a self-referential struct.
async fn receive_or_drop<T>(
    index: usize,
    mut receiver: Receiver<T>,
) -> (usize, Option<(Receiver<T>, T)>) {
    (index, receiver.recv().await.map(|item| (receiver, item)))
}

/// This is not simply the merge of `ReceiverStream` because we need to know if the source has quit.
pub fn receivers_stream<T>(receivers: Vec<Receiver<T>>) -> impl Stream<Item = (usize, Option<T>)> {
    let mut futures = receivers
        .into_iter()
        .enumerate()
        .map(|(index, receiver)| Box::pin(receive_or_drop(index, receiver)))
        .collect::<Vec<_>>();

    stream! {
        while !futures.is_empty() {
            match select_all(futures).await {
                ((index, Some((receiver, item))), _, remaining) => {
                    yield (index, Some(item));
                    futures = remaining;
                    // Can we somehow remove the allocation here?
                    futures.push(Box::pin(receive_or_drop(index, receiver)));
                }
                ((index, None), _, remaining) => {
                    yield (index, None);
                    futures = remaining;
                }
            }
        }
    }
}
