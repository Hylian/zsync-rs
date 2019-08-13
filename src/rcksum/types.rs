use std::hash::{Hash, Hasher};
use std::num::Wrapping;
use md4::{Md4, Digest};

#[derive(Copy, Clone, Debug, Default)]
pub struct Rsum(pub u8, pub u8);

impl PartialEq for Rsum {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        (self.0 == other.0) && (self.1 == other.1)
    }
}

impl Eq for Rsum {}

impl Hash for Rsum {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(<u16>::from(self.0) + <u16>::from(self.1));
    }
}

impl Rsum {
    // Calculate the checksum of a block
    pub fn calculate(data: &[u8]) -> Self {
        let result = data.iter().fold((Rsum(0, 0), data.len() as u8), |acc, x| {
            let a = Wrapping((acc.0).0);
            let b = Wrapping((acc.0).1);
            let o = Wrapping(acc.1);
            let x = Wrapping(*x);
            (Rsum((a + x).0, ((b + o) * x).0), x.0 - 1)
        });
        result.0
    }

    // Update the rolling checksum with the next byte
    pub fn update(self, old: u8, new: u8, blockshift: i32) -> Self {
        Rsum(new - old, self.1 + self.0 - (old << blockshift))
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct PartialChecksum {
    pub value: MD4Digest,
    pub length: usize,
}

impl PartialEq for PartialChecksum {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            (0 == libc::memcmp(self.value.0.as_ptr() as *const _, other.value.0.as_ptr() as *const _, self.length))
        }
    }
}

impl Eq for PartialChecksum {}

impl Hash for PartialChecksum {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(&self.value.0[0..self.length], state);
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct MD4Digest(pub [u8; 16]);

impl Default for MD4Digest {
    fn default() -> Self { MD4Digest([0; 16]) }
}

impl From<[u8; 16]> for MD4Digest {
    fn from(x: [u8; 16]) -> Self {
        MD4Digest(x)
    }
}

impl MD4Digest {
    pub fn calculate(data: &[u8]) -> Self {
        let mut hasher = Md4::new();
        hasher.input(data);
        let mut result = MD4Digest([0; 16]);
        result.0.copy_from_slice(hasher.result().as_slice());
        result
    }
}
