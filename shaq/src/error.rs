use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    InvalidMagic,
    InvalidVersion {
        expected: u32,
        actual: u32,
    },
    InvalidBufferSize,
    InvalidRegionAlignment {
        minimum: usize,
        actual: usize,
    },
    Allocation(std::alloc::Layout),
    Io(std::io::Error),
    Mmap(std::io::Error),
    ProducerSlotsExhausted,
    ConsumerSlotsExhausted,
    /// A recovery index was out of range for the queue's slot count.
    InvalidIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    Timeout,
}

impl std::error::Error for Error {}
impl std::error::Error for WaitError {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid magic"),
            Self::InvalidVersion { expected, actual } => write!(
                f,
                "invalid version; expected={}.{}; found={}.{}",
                expected >> 16,
                expected & 0xFFFF,
                actual >> 16,
                actual & 0xFFFF,
            ),
            Self::InvalidBufferSize => write!(f, "invalid buffer size"),
            Self::InvalidRegionAlignment { minimum, actual } => write!(
                f,
                "invalid region alignment; minimum={minimum}; actual={actual}"
            ),
            Self::Allocation(layout) => write!(
                f,
                "allocation; size={}; align={}",
                layout.size(),
                layout.align()
            ),
            Self::Io(err) => write!(f, "io; err={err}"),
            Self::Mmap(err) => write!(f, "mmap; err={err}"),
            Self::ProducerSlotsExhausted => write!(f, "producer slots exhausted"),
            Self::ConsumerSlotsExhausted => write!(f, "consumer slots exhausted"),
            Self::InvalidIndex => write!(f, "invalid index"),
        }
    }
}

impl Display for WaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "wait timed out"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_version_display() {
        let expected: u32 = 1u32 << 16; // 1.0
        let actual: u32 = (3u32 << 16) | 7; // 3.7
        let err = Error::InvalidVersion { expected, actual };
        assert_eq!(err.to_string(), "invalid version; expected=1.0; found=3.7");
    }

    #[test]
    fn test_wait_timeout_display() {
        assert_eq!(WaitError::Timeout.to_string(), "wait timed out");
    }
}
