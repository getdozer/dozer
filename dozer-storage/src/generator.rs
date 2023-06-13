use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    ptr::null,
    rc::Rc,
    task::{self, RawWaker, RawWakerVTable, Waker},
};

use pin_project::pin_project;

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum GeneratorState<Y, R> {
    Yielded(Y),
    Complete(R),
}

/// Exactly `std::ops::Generator`, with an additional `into_iter`.
pub trait Generator {
    type Yield;

    type Return;

    fn resume(self: Pin<&mut Self>) -> GeneratorState<Self::Yield, Self::Return>;

    fn into_iter(self) -> GeneratorIterator<Self>
    where
        Self: Sized + Unpin,
    {
        GeneratorIterator::new(self)
    }
}

impl<G: Generator> Generator for Pin<Box<G>> {
    type Yield = G::Yield;

    type Return = G::Return;

    fn resume(self: Pin<&mut Self>) -> GeneratorState<Self::Yield, Self::Return> {
        unsafe { self.get_unchecked_mut() }.as_mut().resume()
    }
}

impl<G: Generator> Generator for Pin<&mut G> {
    type Yield = G::Yield;

    type Return = G::Return;

    fn resume(self: Pin<&mut Self>) -> GeneratorState<Self::Yield, Self::Return> {
        unsafe { self.get_unchecked_mut() }.as_mut().resume()
    }
}

/// This `Iterator` yields the values yielded by a `Generator` until it's completed.
pub struct GeneratorIterator<G: Generator + Unpin> {
    generator: G,
    state: GeneratorIteratorState<G::Return>,
}

enum GeneratorIteratorState<T> {
    Yielding,
    Completed(T),
    CompletedAndTaken,
}

impl<T> GeneratorIteratorState<T> {
    fn take_return(&mut self) -> Option<T> {
        if matches!(self, Self::Yielding) || matches!(self, Self::CompletedAndTaken) {
            None
        } else {
            match std::mem::replace(self, Self::CompletedAndTaken) {
                Self::Completed(value) => Some(value),
                _ => unreachable!(),
            }
        }
    }
}

impl<G: Generator + Unpin> Iterator for GeneratorIterator<G> {
    type Item = G::Yield;

    fn next(&mut self) -> Option<Self::Item> {
        if let GeneratorIteratorState::Yielding = self.state {
            match Pin::new(&mut self.generator).resume() {
                GeneratorState::Yielded(value) => Some(value),
                GeneratorState::Complete(value) => {
                    self.state = GeneratorIteratorState::Completed(value);
                    None
                }
            }
        } else {
            None
        }
    }
}

impl<G: Generator + Unpin> GeneratorIterator<G> {
    pub fn new(generator: G) -> Self {
        Self {
            generator,
            state: GeneratorIteratorState::Yielding,
        }
    }

    pub fn into_generator(self) -> G {
        self.generator
    }

    pub fn take_return(&mut self) -> Option<G::Return> {
        self.state.take_return()
    }
}

/// The generic parameter `Arg` is a workaround from <https://github.com/rust-lang/rust/issues/60074#issuecomment-779452213>.
pub trait IntoGenerator<Arg = ()> {
    type Generator: Generator;

    fn into_generator(self) -> Self::Generator;

    fn into_iter(self) -> GeneratorIterator<Self::Generator>
    where
        Self: Sized,
        Self::Generator: Unpin,
    {
        GeneratorIterator::new(self.into_generator())
    }
}

/// This future is polled once and then returns `Poll::Ready(())` forever.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct Once {
    done: bool,
}

impl Future for Once {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let done = self.project().done;
        if *done {
            task::Poll::Ready(())
        } else {
            *done = true;
            task::Poll::Pending
        }
    }
}

#[derive(Debug)]
/// Use this to yield from an async function to make the function a generator.
///
/// ```rust
/// use dozer_storage::generator::{FutureGeneratorContext, Generator, GeneratorState, IntoGenerator};
///
/// async fn fib(context: FutureGeneratorContext<i32>) {
///    let mut x = 0;
///     let mut y = 1;
///     loop {
///         context.yield_(x).await;
///         let tmp = x + y;
///         x = y;
///         y = tmp;
///     }
/// }
///
/// let generator = std::pin::pin!(fib.into_generator());
/// assert_eq!(
///     generator
///         .into_iter()
///         .take(10)
///         .collect::<Vec<_>>(),
///     vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
/// );
/// ```
pub struct FutureGeneratorContext<T> {
    value: Rc<RefCell<Option<T>>>,
}

impl<T> Clone for FutureGeneratorContext<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
        }
    }
}

impl<T> FutureGeneratorContext<T> {
    pub fn yield_(&self, value: T) -> Once {
        let mut cell = self.value.borrow_mut();
        assert!(
            cell.is_none(),
            "Make sure to await the future returned by `yield_`"
        );
        *cell = Some(value);
        Once { done: false }
    }
}

#[pin_project]
pub struct FutureGenerator<Y, R, F: Future<Output = R>> {
    #[pin]
    future: F,
    waker: Waker,
    context: FutureGeneratorContext<Y>,
}

impl<Y, R, F: Future<Output = R>> Generator for FutureGenerator<Y, R, F> {
    type Yield = Y;

    type Return = R;

    fn resume(self: Pin<&mut Self>) -> GeneratorState<Self::Yield, Self::Return> {
        let this = self.project();
        let result = match this.future.poll(&mut task::Context::from_waker(this.waker)) {
            std::task::Poll::Ready(value) => GeneratorState::Complete(value),
            std::task::Poll::Pending => GeneratorState::Yielded(
                this.context
                    .value
                    .take()
                    .expect("Generator should only await using Context::yield"),
            ),
        };
        result
    }
}

impl<Y, R, Fut: Future<Output = R>, Fn: FnOnce(FutureGeneratorContext<Y>) -> Fut> IntoGenerator<Y>
    for Fn
{
    type Generator = FutureGenerator<Y, R, Fut>;

    fn into_generator(self) -> Self::Generator {
        let context = FutureGeneratorContext {
            value: Rc::new(RefCell::new(None)),
        };
        let future = self(context.clone());
        let waker = unsafe { Waker::from_raw(RawWaker::new(null(), &VTABLE)) };
        FutureGenerator {
            future,
            waker,
            context,
        }
    }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(
    raw_waker_clone,
    raw_waker_wake,
    raw_waker_wake_by_ref,
    raw_waker_drop,
);

unsafe fn raw_waker_clone(_: *const ()) -> RawWaker {
    RawWaker::new(null(), &VTABLE)
}

unsafe fn raw_waker_wake(_: *const ()) {}

unsafe fn raw_waker_wake_by_ref(_: *const ()) {}

unsafe fn raw_waker_drop(_: *const ()) {}
