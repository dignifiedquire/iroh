//! Async rust task spawning and utilities that work natively (using tokio) and in browsers
//! (using wasm-bindgen-futures).

#[cfg(not(wasm_browser))]
mod tokio;
#[cfg(wasm_browser)]
mod wasm;

#[cfg(not(wasm_browser))]
pub use tokio::*;
#[cfg(wasm_browser)]
pub use wasm::*;
