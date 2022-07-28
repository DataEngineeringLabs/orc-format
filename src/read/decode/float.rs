use crate::error::Error;

/// Sealead trait to generically represent f32 and f64.
pub trait Float: Default + Copy + private::Sealed {
    type Bytes: AsRef<[u8]> + AsMut<[u8]> + Default;
    fn from_le_bytes(bytes: Self::Bytes) -> Self;
}

mod private {
    pub trait Sealed {} // Users in other crates cannot name this trait.
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

impl Float for f32 {
    type Bytes = [u8; 4];

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from_le_bytes(bytes)
    }
}

impl Float for f64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from_le_bytes(bytes)
    }
}

/// An iterator
pub struct FloatIter<'a, T: Float, R: std::io::Read> {
    reader: &'a mut R,
    remaining: usize,
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Float, R: std::io::Read> FloatIter<'a, T, R> {
    #[inline]
    pub fn new(reader: &'a mut R, length: usize) -> Self {
        Self {
            reader,
            remaining: length,
            phantom: Default::default(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.remaining
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, T: Float, R: std::io::Read> Iterator for FloatIter<'a, T, R> {
    type Item = Result<T, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut chunk: T::Bytes = Default::default();
        let error = self.reader.read_exact(chunk.as_mut());
        if error.is_err() {
            return Some(Err(Error::DecodeFloat));
        };
        self.remaining -= 1;
        Some(Ok(T::from_le_bytes(chunk)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}
