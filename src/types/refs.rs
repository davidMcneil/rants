use futures::lock::MutexGuard;
use owning_ref::{OwningRef, OwningRefMut, StableAddress};
use std::ops::{Deref, DerefMut};

use crate::SyncClient;

// A wrapper needed to implement the `StableAddress` trait to use `OwningRef`
pub struct StableMutexGuard<'a, T: ?Sized>(pub(crate) MutexGuard<'a, T>);

unsafe impl<'a, T: ?Sized> StableAddress for StableMutexGuard<'a, T> {}

impl<T: ?Sized> Deref for StableMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> DerefMut for StableMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

/// A type for returning a reference to data behind [`Client`](struct.Client.html)'s internal mutex
///
/// The mutex is held for the entire lifetime of this reference so the lifetime of the reference
/// should not be long lived.
pub struct ClientRef<'a, T: ?Sized>(pub(crate) OwningRef<StableMutexGuard<'a, SyncClient>, T>);

impl<'a, T: ?Sized> Deref for ClientRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.0
    }
}

/// A type for returning a mutable reference to data behind [`Client`](struct.Client.html)'s internal mutex
///
/// The mutex is held for the entire lifetime of this reference so the lifetime of the reference
/// should not be long lived.
pub struct ClientRefMut<'a, T: ?Sized>(
    pub(crate) OwningRefMut<StableMutexGuard<'a, SyncClient>, T>,
);

impl<'a, T: ?Sized> Deref for ClientRefMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.0
    }
}

impl<'a, T: ?Sized> DerefMut for ClientRefMut<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}
