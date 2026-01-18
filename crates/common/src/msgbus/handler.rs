// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Message handler functionality for the message bus system.
//!
//! This module provides a trait and implementations for handling messages
//! in a type-safe manner, enabling both typed and untyped message processing.

use std::{
    any::{Any, type_name},
    fmt::Debug,
    marker::PhantomData,
    rc::Rc,
};

use nautilus_core::UUID4;
use ustr::Ustr;

pub trait MessageHandler: Any {
    /// Returns the unique identifier for this handler.
    fn id(&self) -> Ustr;
    /// Handles a message of any type.
    fn handle(&self, message: &dyn Any);
    /// Returns this handler as a trait object.
    fn as_any(&self) -> &dyn Any;
}

impl PartialEq for dyn MessageHandler {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for dyn MessageHandler {}

#[derive(Debug)]
pub(crate) struct TypedMessageHandler<T: 'static + ?Sized, F: Fn(&T) + 'static> {
    id: Ustr,
    callback: F,
    _phantom: PhantomData<T>,
}

impl<T: 'static, F: Fn(&T) + 'static> TypedMessageHandler<T, F> {
    /// Creates a new handler with an optional custom ID.
    pub fn new<S: AsRef<str>>(id: Option<S>, callback: F) -> Self {
        let id_ustr = id.map_or_else(
            || generate_handler_id(&callback),
            |s| Ustr::from(s.as_ref()),
        );

        Self {
            id: id_ustr,
            callback,
            _phantom: PhantomData,
        }
    }

    /// Creates a new handler with an auto-generated ID.
    pub fn from(callback: F) -> Self {
        Self::new::<Ustr>(None, callback)
    }
}

impl<T: 'static, F: Fn(&T) + 'static> MessageHandler for TypedMessageHandler<T, F> {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, message: &dyn Any) {
        if let Some(typed_msg) = message.downcast_ref::<T>() {
            (self.callback)(typed_msg);
        } else {
            log::error!(
                "TypedMessageHandler downcast failed: expected {} got {:?}",
                type_name::<T>(),
                message.type_id()
            );
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<F: Fn(&dyn Any) + 'static> TypedMessageHandler<dyn Any, F> {
    /// Creates a new handler for dynamic Any messages with an optional custom ID.
    pub fn new_any<S: AsRef<str>>(id: Option<S>, callback: F) -> Self {
        let id_ustr = id.map_or_else(
            || generate_handler_id(&callback),
            |s| Ustr::from(s.as_ref()),
        );

        Self {
            id: id_ustr,
            callback,
            _phantom: PhantomData,
        }
    }

    /// Creates a handler for Any messages with an auto-generated ID.
    pub fn with_any(callback: F) -> Self {
        Self::new_any::<&str>(None, callback)
    }
}

impl<F: Fn(&dyn Any) + 'static> MessageHandler for TypedMessageHandler<dyn Any, F> {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, message: &dyn Any) {
        (self.callback)(message);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn generate_handler_id<T: 'static + ?Sized, F: 'static + Fn(&T)>(callback: &F) -> Ustr {
    let callback_ptr = std::ptr::from_ref(callback);
    let uuid = UUID4::new();
    Ustr::from(&format!("<{callback_ptr:?}>-{uuid}"))
}

// ShareableMessageHandler contains Rc<dyn MessageHandler> which is not Send/Sync.
// This is intentional - message handlers are designed for single-threaded use within
// each async runtime. The MessageBus uses thread-local storage to ensure each thread
// gets its own handlers, eliminating the need for unsafe Send/Sync implementations.
#[repr(transparent)]
#[derive(Clone)]
pub struct ShareableMessageHandler(pub Rc<dyn MessageHandler>);

impl ShareableMessageHandler {
    pub fn id(&self) -> Ustr {
        self.0.id()
    }

    /// Creates a `ShareableMessageHandler` from a typed closure.
    ///
    /// This is a convenience method that wraps the common pattern of creating
    /// a typed message handler and wrapping it in a shareable handler.
    pub fn from_typed<T, F>(f: F) -> Self
    where
        T: 'static,
        F: Fn(&T) + 'static,
    {
        Self(Rc::new(TypedMessageHandler::from(f)))
    }

    /// Creates a `ShareableMessageHandler` from an Any-typed closure.
    ///
    /// This is a convenience method for handlers that need to work with
    /// custom data types via `dyn Any`.
    pub fn from_any<F>(f: F) -> Self
    where
        F: Fn(&dyn Any) + 'static,
    {
        Self(Rc::new(TypedMessageHandler::with_any(f)))
    }
}

impl Debug for ShareableMessageHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ShareableMessageHandler))
            .field("id", &self.0.id())
            .field("type", &std::any::type_name::<Self>().to_string())
            .finish()
    }
}

impl From<Rc<dyn MessageHandler>> for ShareableMessageHandler {
    fn from(value: Rc<dyn MessageHandler>) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_typed_message_handler_with_custom_id() {
        let handler = TypedMessageHandler::new(Some("custom-id"), |_msg: &i32| {});

        assert_eq!(handler.id(), Ustr::from("custom-id"));
    }

    #[rstest]
    fn test_typed_message_handler_auto_generated_id() {
        let handler = TypedMessageHandler::from(|_msg: &i32| {});

        // Auto-generated ID should contain a UUID pattern
        let id = handler.id().to_string();
        assert!(id.contains('-')); // UUIDs contain dashes
    }

    #[rstest]
    fn test_typed_message_handler_handles_correct_type() {
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedMessageHandler::new(Some("test"), move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        handler.handle(&"hello".to_string() as &dyn Any);
        handler.handle(&"world".to_string() as &dyn Any);

        assert_eq!(*received.borrow(), vec!["hello", "world"]);
    }

    #[rstest]
    fn test_typed_message_handler_ignores_wrong_type() {
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedMessageHandler::new(Some("test"), move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        // Pass wrong type - should be ignored (logs error)
        handler.handle(&42i32 as &dyn Any);

        assert!(received.borrow().is_empty());
    }

    #[rstest]
    fn test_typed_message_handler_any() {
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedMessageHandler::with_any(move |msg: &dyn Any| {
            if let Some(s) = msg.downcast_ref::<String>() {
                received_clone.borrow_mut().push(format!("String:{s}"));
            } else if let Some(i) = msg.downcast_ref::<i32>() {
                received_clone.borrow_mut().push(format!("i32:{i}"));
            }
        });

        handler.handle(&"test".to_string() as &dyn Any);
        handler.handle(&42i32 as &dyn Any);

        assert_eq!(*received.borrow(), vec!["String:test", "i32:42"]);
    }

    #[rstest]
    fn test_shareable_message_handler_id() {
        let handler = TypedMessageHandler::new(Some("shareable-test"), |_: &i32| {});
        let shareable = ShareableMessageHandler(Rc::new(handler));

        assert_eq!(shareable.id(), Ustr::from("shareable-test"));
    }

    #[rstest]
    fn test_shareable_message_handler_debug() {
        let handler = TypedMessageHandler::new(Some("debug-test"), |_: &i32| {});
        let shareable = ShareableMessageHandler(Rc::new(handler));

        let debug_str = format!("{shareable:?}");
        assert!(debug_str.contains("ShareableMessageHandler"));
        assert!(debug_str.contains("debug-test"));
    }

    #[rstest]
    fn test_shareable_message_handler_from_rc() {
        let handler: Rc<dyn MessageHandler> =
            Rc::new(TypedMessageHandler::new(Some("from-rc"), |_: &i32| {}));
        let shareable: ShareableMessageHandler = handler.into();

        assert_eq!(shareable.id(), Ustr::from("from-rc"));
    }

    #[rstest]
    fn test_shareable_message_handler_clone() {
        let received = Rc::new(RefCell::new(0));
        let received_clone = received.clone();

        let handler = TypedMessageHandler::new(Some("clone-test"), move |_: &i32| {
            *received_clone.borrow_mut() += 1;
        });
        let shareable = ShareableMessageHandler(Rc::new(handler));
        let cloned = shareable.clone();

        // Both should point to same handler
        shareable.0.handle(&1i32 as &dyn Any);
        cloned.0.handle(&2i32 as &dyn Any);

        assert_eq!(*received.borrow(), 2);
    }

    #[rstest]
    fn test_message_handler_equality() {
        let handler1: Rc<dyn MessageHandler> =
            Rc::new(TypedMessageHandler::new(Some("same-id"), |_: &i32| {}));
        let handler2: Rc<dyn MessageHandler> =
            Rc::new(TypedMessageHandler::new(Some("same-id"), |_: &i32| {}));
        let handler3: Rc<dyn MessageHandler> =
            Rc::new(TypedMessageHandler::new(Some("different-id"), |_: &i32| {}));

        assert!((*handler1).eq(&*handler2));
        assert!(!(*handler1).eq(&*handler3));
    }

    #[rstest]
    fn test_typed_message_handler_new_any() {
        let handler = TypedMessageHandler::new_any(Some("new-any-test"), |_: &dyn Any| {});
        assert_eq!(handler.id(), Ustr::from("new-any-test"));
    }

    #[rstest]
    fn test_shareable_message_handler_from_typed() {
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = ShareableMessageHandler::from_typed(move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        handler.0.handle(&"hello".to_string() as &dyn Any);
        handler.0.handle(&"world".to_string() as &dyn Any);

        assert_eq!(*received.borrow(), vec!["hello", "world"]);
    }

    #[rstest]
    fn test_shareable_message_handler_from_any() {
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = ShareableMessageHandler::from_any(move |msg: &dyn Any| {
            if let Some(s) = msg.downcast_ref::<String>() {
                received_clone.borrow_mut().push(format!("String:{s}"));
            } else if let Some(i) = msg.downcast_ref::<i32>() {
                received_clone.borrow_mut().push(format!("i32:{i}"));
            }
        });

        handler.0.handle(&"test".to_string() as &dyn Any);
        handler.0.handle(&42i32 as &dyn Any);

        assert_eq!(*received.borrow(), vec!["String:test", "i32:42"]);
    }
}
