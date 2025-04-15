//! Callback function management for handling progress updates
//!
//! This module provides functionality for:
//! - Creating and managing thread-safe callback functions
//! - Cloning and sharing callbacks between threads
//! - Wrapping closures into callback functions
//! - Executing callbacks with progress values

use std::sync::{Arc, Mutex};

/// Type alias for a boxed callback function that takes a u64 argument and is Send
pub type CallbackFunInner = Box<dyn FnMut(&[u8]) + Send>;

/// A thread-safe wrapper around a callback function
///
/// This struct provides a way to safely share and execute callback functions
/// across multiple threads. It uses Arc and Mutex to ensure thread-safety.
pub struct CallbackFun {
    /// The inner callback function wrapped in Arc<Mutex>
    inner: Arc<Mutex<CallbackFunInner>>,
}

/// Clone implementation for CallbackFun
///
/// Creates a new CallbackFun instance that shares the same underlying callback
/// function with the original instance.
impl Clone for CallbackFun {
    fn clone(&self) -> Self {
        CallbackFun {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl CallbackFun {
    /// Creates a new `CallbackFun` instance from a given callback function.
    ///
    /// # Arguments
    /// * `f` - A boxed callback function that takes a `u64` argument and is `Send`.
    ///
    /// # Returns
    /// A new `CallbackFun` instance.
    pub fn new(f: CallbackFunInner) -> Self {
        CallbackFun {
            inner: Arc::new(Mutex::new(f)),
        }
    }

    /// Calls the inner callback function with the provided argument.
    ///
    /// # Arguments
    /// * `arg` - A `u64` argument to pass to the callback function.
    pub fn call(&self, arg: &[u8]) {
        let mut f = self.inner.lock().unwrap();
        f(arg);
    }

    /// Wraps a closure into a `CallbackFun` instance.
    ///
    /// # Arguments
    /// * `closure` - A closure that takes a `u64` argument and is `Send`.
    ///
    /// # Returns
    /// A new `CallbackFun` instance wrapping the provided closure.
    pub fn wrap<F>(closure: F) -> Self
    where
        F: FnMut(&[u8]) + Send + 'static,
    {
        CallbackFun::new(Box::new(closure))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn new_creates_instance() {
        let callback = CallbackFun::new(Box::new(|_| {}));
        assert_eq!(Arc::strong_count(&callback.inner), 1);
    }

    #[test]
    fn call_executes_callback() {
        let called = Arc::new(Mutex::new(false));
        let called_clone = Arc::clone(&called);

        let callback = CallbackFun::new(Box::new(move |_| {
            let mut called = called_clone.lock().unwrap();
            *called = true;
        }));

        callback.call(&[42]);
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn clone_creates_new_instance() {
        let callback = CallbackFun::new(Box::new(|_| {}));
        let callback_clone = callback.clone();
        assert_eq!(Arc::strong_count(&callback.inner), 2);
        assert_eq!(Arc::strong_count(&callback_clone.inner), 2);
    }

    #[test]
    fn wrap_creates_instance_from_closure() {
        let callback = CallbackFun::wrap(|_| {});
        assert_eq!(Arc::strong_count(&callback.inner), 1);
    }

    #[test]
    fn call_with_different_values() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let callback = CallbackFun::new(Box::new(move |val| {
            let mut values = values_clone.lock().expect("Failed to lock values");
            values.push(val.to_vec());
        }));

        callback.call(&[1]);
        callback.call(&[2]);
        callback.call(&[3]);

        let values = values.lock().unwrap();
        assert_eq!(*values, vec![vec![1], vec![2], vec![3]]);
    }
}
