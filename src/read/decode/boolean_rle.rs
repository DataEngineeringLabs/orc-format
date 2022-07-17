use crate::Error;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BooleanRun<'a> {
    Run(u8, u8),
    Literals(&'a [u8]),
}

pub struct BooleanRleRunIter<'a> {
    stream: &'a [u8],
}

impl<'a> BooleanRleRunIter<'a> {
    pub fn new(stream: &'a [u8]) -> Self {
        Self { stream }
    }
}

impl<'a> Iterator for BooleanRleRunIter<'a> {
    type Item = Result<BooleanRun<'a>, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let header = *self.stream.first()?;
        self.stream = &self.stream[1..];
        let header = i8::from_le_bytes([header]);
        if header < 0 {
            let length = (-header) as usize;
            if length > self.stream.len() {
                return Some(Err(Error::RleLiteralTooLarge));
            }
            let (literals, remaining) = self.stream.split_at(length);
            self.stream = remaining;
            Some(Ok(BooleanRun::Literals(literals)))
        } else {
            let length = (header + 3) as u8;
            let value = self.stream[0];
            self.stream = &self.stream[1..];
            Some(Ok(BooleanRun::Run(value, length)))
        }
    }
}

pub struct BooleanIter<'a> {
    iter: BooleanRleRunIter<'a>,
    current: Option<BooleanRun<'a>>,
    position: u8,
    remaining: usize,
}

impl<'a> BooleanIter<'a> {
    pub fn new(stream: &'a [u8], length: usize) -> Self {
        Self {
            iter: BooleanRleRunIter::new(stream),
            current: None,
            position: 0,
            remaining: length,
        }
    }
}

impl<'a> Iterator for BooleanIter<'a> {
    type Item = Result<bool, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(run) = &self.current {
            match run {
                BooleanRun::Run(value, repetitions) => {
                    let repetitions = *repetitions;
                    let mask = 128u8 >> self.position;
                    let result = value & mask == mask;
                    self.position += 1;
                    if self.remaining == 0 {
                        self.current = None;
                        return None;
                    } else {
                        self.remaining -= 1;
                    }
                    if self.position == 7 {
                        if repetitions == 0 {
                            self.current = None;
                        } else {
                            self.current = Some(BooleanRun::Run(*value, repetitions - 1));
                        }
                        self.position = 0;
                    }
                    Some(Ok(result))
                }
                BooleanRun::Literals(bytes) => {
                    let mask = 128u8 >> self.position;
                    let result = bytes[0] & mask == mask;
                    self.position += 1;
                    if self.remaining == 0 {
                        self.current = None;
                        return None;
                    } else {
                        self.remaining -= 1;
                    }
                    if self.position == 7 {
                        if bytes.len() == 1 {
                            self.current = None;
                        } else {
                            self.current = Some(BooleanRun::Literals(&bytes[1..]));
                        }
                        self.position = 0;
                    }
                    Some(Ok(result))
                }
            }
        } else if self.remaining > 0 {
            match self.iter.next()? {
                Ok(run) => {
                    self.current = Some(run);
                    self.next()
                }
                Err(e) => {
                    self.remaining = 0;
                    Some(Err(e))
                }
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
