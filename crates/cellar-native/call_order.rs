//! Caller-local cell delivery chains. Failure hands an unfinished predecessor
//! to the successor; cancellation of a wait cannot remove that predecessor.
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[derive(Default)]
struct PlaceState {
    finished: bool,
    forward: Option<Arc<Place>>,
    waiters: BTreeMap<u64, Waker>,
    next: u64,
}

#[derive(Default)]
struct Place(Mutex<PlaceState>);

impl Place {
    fn finish(&self, forward: Option<Arc<Self>>) {
        let wake = {
            let mut state = self.0.lock().unwrap();
            debug_assert!(!state.finished);
            state.finished = true;
            state.forward = forward;
            std::mem::take(&mut state.waiters)
        };
        for (_, waker) in wake {
            waker.wake();
        }
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        // Long runs of abandoned calls form a chain of completed forwards.
        // Reclaim unique links iteratively instead of overflowing a host stack.
        let mut next = self.0.get_mut().unwrap().forward.take();
        while let Some(place) = next {
            match Arc::try_unwrap(place) {
                Ok(mut place) => {
                    next = place.0.get_mut().unwrap().forward.take();
                }
                Err(_) => break,
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct CallChains(Arc<Mutex<HashMap<String, Chain>>>);

struct Chain {
    tail: Arc<Place>,
    live: usize,
}

impl CallChains {
    pub fn enter(&self, cell: &str) -> CallOrder {
        self.enter_inner(cell, None)
    }

    /// Keep the originating host event alive exactly as long as its order
    /// token. The primitive need not know the event's runtime or engine type.
    pub fn enter_with_owner(&self, cell: &str, owner: Arc<dyn Any + Send + Sync>) -> CallOrder {
        self.enter_inner(cell, Some(owner))
    }

    fn enter_inner(&self, cell: &str, owner: Option<Arc<dyn Any + Send + Sync>>) -> CallOrder {
        let release = Arc::new(Place::default());
        let ahead = {
            let mut chains = self.0.lock().unwrap();
            match chains.get_mut(cell) {
                Some(chain) => {
                    chain.live = chain
                        .live
                        .checked_add(1)
                        .expect("call-order live token count exhausted");
                    Some(std::mem::replace(&mut chain.tail, release.clone()))
                }
                None => {
                    chains.insert(
                        cell.to_owned(),
                        Chain {
                            tail: release.clone(),
                            live: 1,
                        },
                    );
                    None
                }
            }
        };
        CallOrder {
            chains: self.clone(),
            cell: cell.to_owned(),
            ahead,
            release: Some(release),
            delivered: false,
            _owner: owner,
        }
    }

    pub fn pending_cells(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

pub struct CallOrder {
    chains: CallChains,
    cell: String,
    ahead: Option<Arc<Place>>,
    release: Option<Arc<Place>>,
    delivered: bool,
    _owner: Option<Arc<dyn Any + Send + Sync>>,
}

impl CallOrder {
    pub fn wait(&mut self) -> CallWait<'_> {
        CallWait {
            order: self,
            registration: None,
        }
    }

    pub fn delivered(&mut self) {
        self.delivered = true;
    }
}

impl Drop for CallOrder {
    fn drop(&mut self) {
        let Some(release) = self.release.take() else {
            return;
        };
        let removed = {
            let mut chains = self.chains.0.lock().unwrap();
            let chain = chains
                .get_mut(&self.cell)
                .expect("a live token owns its caller chain");
            chain.live -= 1;
            // A failed latest token can still precede a future call. Preserve
            // its forward while any older token may still be delivered.
            if chain.live == 0 {
                chains.remove(&self.cell)
            } else {
                None
            }
        };
        drop(removed);
        let forward = if self.delivered {
            None
        } else {
            self.ahead.take()
        };
        release.finish(forward);
    }
}

pub struct CallWait<'a> {
    order: &'a mut CallOrder,
    registration: Option<(Arc<Place>, u64)>,
}

impl CallWait<'_> {
    fn unregister(&mut self) {
        if let Some((place, id)) = self.registration.take() {
            let removed = place.0.lock().unwrap().waiters.remove(&id);
            drop(removed);
        }
    }
}

impl Future for CallWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // Completed forwarding chains can be long. Yield bounded work while
        // retaining the next predecessor, rather than monopolizing a turn.
        for _ in 0..64 {
            let Some(place) = this.order.ahead.clone() else {
                this.unregister();
                return Poll::Ready(());
            };
            let waker = cx.waker().clone();
            let mut state = place.0.lock().unwrap();
            if state.finished {
                let next = state.forward.clone();
                drop(state);
                this.unregister();
                this.order.ahead = next;
                continue;
            }
            let id = match &this.registration {
                Some((registered, id)) => {
                    debug_assert!(Arc::ptr_eq(registered, &place));
                    *id
                }
                None => {
                    let id = state.next;
                    state.next = state
                        .next
                        .checked_add(1)
                        .expect("call-order waiter identity exhausted");
                    this.registration = Some((place.clone(), id));
                    id
                }
            };
            let replaced = state.waiters.insert(id, waker);
            drop(state);
            drop(replaced);
            return Poll::Pending;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for CallWait<'_> {
    fn drop(&mut self) {
        self.unregister();
    }
}
