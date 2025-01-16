//! Sleep and timeout utilities that work natively (via tokio) and in the browser.

#[cfg(not(wasm_browser))]
pub use tokio::time::*;

#[cfg(wasm_browser)]
pub use wasm::{sleep, Duration, Instant, Sleep};

#[cfg(wasm_browser)]
mod wasm {
    use futures_util::task::AtomicWaker;
    use send_wrapper::SendWrapper;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, Ordering::Relaxed},
            Arc,
        },
        task::{Context, Poll},
    };
    use wasm_bindgen::{closure::Closure, prelude::wasm_bindgen, JsCast, JsValue};

    pub use web_time::{Duration, Instant};

    #[derive(Debug)]
    pub struct Sleep {
        deadline: Instant,
        triggered: Flag,
        timeout_id: SendWrapper<JsValue>,
    }

    pub fn sleep(duration: Duration) -> Sleep {
        let now = Instant::now();
        let deadline = now + duration;
        let triggered = Flag::new();

        let closure = Closure::once({
            let triggered = triggered.clone();
            move || triggered.signal()
        });

        let timeout_id = SendWrapper::new(
            set_timeout(
                closure.into_js_value().unchecked_into(),
                duration.as_millis() as i32,
            )
            .expect("missing setTimeout function on globalThis"),
        );

        Sleep {
            deadline,
            triggered,
            timeout_id,
        }
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.triggered).poll_signaled(cx)
        }
    }

    impl Drop for Sleep {
        fn drop(&mut self) {
            // If not, then in the worst case we're leaking a timeout
            if self.timeout_id.valid() {
                clear_timeout(self.timeout_id.as_ref().clone()).ok();
            }
        }
    }

    impl Sleep {
        pub fn reset(mut self: Pin<&mut Self>, deadline: Instant) {
            let duration = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            let triggered = Flag::new();

            let closure = Closure::once({
                let triggered = triggered.clone();
                move || triggered.signal()
            });

            let timeout_id = SendWrapper::new(
                set_timeout(
                    closure.into_js_value().unchecked_into(),
                    duration.as_millis() as i32,
                )
                .expect("missing setTimeout function on globalThis"),
            );

            let mut this = self.as_mut();
            this.deadline = deadline;
            this.triggered = triggered;
            let old_timeout_id = std::mem::replace(&mut this.timeout_id, timeout_id);
            // If not valid, then in the worst case we're leaking a timeout task
            if old_timeout_id.valid() {
                clear_timeout(old_timeout_id.as_ref().clone()).ok();
            }
        }
    }

    // Private impls

    #[derive(Clone, Debug)]
    struct Flag(Arc<Inner>);

    #[derive(Debug)]
    struct Inner {
        waker: AtomicWaker,
        set: AtomicBool,
    }

    impl Flag {
        fn new() -> Self {
            Self(Arc::new(Inner {
                waker: AtomicWaker::new(),
                set: AtomicBool::new(false),
            }))
        }

        fn signal(&self) {
            self.0.set.store(true, Relaxed);
            self.0.waker.wake();
        }

        fn poll_signaled(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            // quick check to avoid registration if already done.
            if self.0.set.load(Relaxed) {
                return Poll::Ready(());
            }

            self.0.waker.register(cx.waker());

            // Need to check condition **after** `register` to avoid a race
            // condition that would result in lost notifications.
            if self.0.set.load(Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    // Wasm-bindgen stuff

    #[wasm_bindgen]
    extern "C" {
        type GlobalScope;

        #[wasm_bindgen(catch, method, js_name = "setTimeout")]
        fn set_timeout_with_callback_and_timeout_and_arguments_0(
            this: &GlobalScope,
            handler: js_sys::Function,
            timeout: i32,
        ) -> Result<JsValue, JsValue>;

        #[wasm_bindgen(catch, method, js_name = "clearTimeout")]
        fn clear_timeout_with_handle(
            this: &GlobalScope,
            timeout_id: JsValue,
        ) -> Result<(), JsValue>;
    }

    fn set_timeout(handler: js_sys::Function, timeout: i32) -> Result<JsValue, JsValue> {
        let global_this = js_sys::global();
        let global_scope = global_this.unchecked_ref::<GlobalScope>();
        global_scope.set_timeout_with_callback_and_timeout_and_arguments_0(handler, timeout)
    }

    fn clear_timeout(timeout_id: JsValue) -> Result<(), JsValue> {
        let global_this = js_sys::global();
        let global_scope = global_this.unchecked_ref::<GlobalScope>();
        global_scope.clear_timeout_with_handle(timeout_id)
    }
}
