//! The `hash` module provides functions for creating SHA-256 hashes.

use bs58;
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::fmt;
use std::mem;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Hash([u8; 32]);

#[derive(Clone, Default)]
pub struct Hasher {
    hasher: Sha256,
}

impl Hasher {
    pub fn hash(&mut self, val: &[u8]) {
        self.hasher.input(val);
    }
    pub fn hashv(&mut self, vals: &[&[u8]]) {
        for val in vals {
            self.hash(val);
        }
    }
    pub fn result(self) -> Hash {
        // At the time of this writing, the sha2 library is stuck on an old version
        // of generic_array (0.9.0). Decouple ourselves with a clone to our version.
        Hash(<[u8; 32]>::try_from(self.hasher.result().as_slice()).unwrap())
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl AsMut<[u8]> for Hash {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseHashError {
    WrongSize,
    Invalid,
}

impl FromStr for Hash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|_| ParseHashError::Invalid)?;
        if bytes.len() != mem::size_of::<Hash>() {
            Err(ParseHashError::WrongSize)
        } else {
            Ok(Hash::new(&bytes))
        }
    }
}

impl Hash {
    pub fn new(hash_slice: &[u8]) -> Self {
        Hash(<[u8; 32]>::try_from(hash_slice).unwrap())
    }
}

/// Return a Sha256 hash for the given data.
pub fn hashv(vals: &[&[u8]]) -> Hash {
    let mut hasher = Hasher::default();
    hasher.hashv(vals);
    hasher.result()
}

/// Return a Sha256 hash for the given data.
pub fn hash(val: &[u8]) -> Hash {
    hashv(&[val])
}

/// Return the hash of the given hash extended with the given value.
pub fn extend_and_hash(id: &Hash, val: &[u8]) -> Hash {
    let mut hash_data = id.as_ref().to_vec();
    hash_data.extend_from_slice(val);
    hash(&hash_data)
}

// Type for representing a bank accounts state.
// Taken by xor of a sha256 of accounts state for lower 32-bytes, and
// then generating the rest of the bytes with a chacha rng init'ed with that state.
// 440 bytes solves the birthday problem when xor'ing of preventing an attacker of
// finding a value or set of values that could be xor'ed to match the bitpattern
// of an existing state value.
const BANK_HASH_BYTES: usize = 440;
#[derive(Clone, Copy)]
pub struct BankHash([u8; BANK_HASH_BYTES]);

impl fmt::Debug for BankHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BankHash {}", hex::encode(&self.0[..32]))
    }
}

impl fmt::Display for BankHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl PartialEq for BankHash {
    fn eq(&self, other: &Self) -> bool {
        self.0[..] == other.0[..]
    }
}
impl Eq for BankHash {}

impl Default for BankHash {
    fn default() -> Self {
        BankHash([0u8; BANK_HASH_BYTES])
    }
}

impl BankHash {
    pub fn from_hash(hash: Hash) -> Self {
        let mut new = BankHash::default();
        new.0[..32].copy_from_slice(hash.as_ref());
        new
    }

    pub fn is_default(&self) -> bool {
        self.0[0..32] == Hash::default().as_ref()[0..32]
    }
}

use serde::{Deserialize, Serialize, Serializer};

impl Serialize for BankHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0[..])
        /*for byte in &self.0[..] {
            serializer.serialize_u8(*byte)?;
        }*/
    }
}

struct BankHashVisitor;
use bincode::deserialize_from;
use std::io::Cursor;

impl<'a> serde::de::Visitor<'a> for BankHashVisitor {
    type Value = BankHash;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Expecting AppendVec")
    }

    #[allow(clippy::mutex_atomic)]
    // Note this does not initialize a valid Mmap in the AppendVec, needs to be done
    // externally
    fn visit_bytes<E>(self, data: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        use serde::de::Error;
        let mut new = BankHash::default();
        let mut rd = Cursor::new(&data[..]);
        for i in 0..440 {
            new.0[i] = deserialize_from(&mut rd).map_err(Error::custom)?;
        }

        Ok(new)
    }
}

impl<'de> Deserialize<'de> for BankHash {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BankHashVisitor)
    }
}

impl AsRef<[u8]> for BankHash {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl AsMut<[u8]> for BankHash {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bs58;

    #[test]
    fn test_hash_fromstr() {
        let hash = hash(&[1u8]);

        let mut hash_base58_str = bs58::encode(hash).into_string();

        assert_eq!(hash_base58_str.parse::<Hash>(), Ok(hash));

        hash_base58_str.push_str(&bs58::encode(hash.0).into_string());
        assert_eq!(
            hash_base58_str.parse::<Hash>(),
            Err(ParseHashError::WrongSize)
        );

        hash_base58_str.truncate(hash_base58_str.len() / 2);
        assert_eq!(hash_base58_str.parse::<Hash>(), Ok(hash));

        hash_base58_str.truncate(hash_base58_str.len() / 2);
        assert_eq!(
            hash_base58_str.parse::<Hash>(),
            Err(ParseHashError::WrongSize)
        );

        let mut hash_base58_str = bs58::encode(hash.0).into_string();
        assert_eq!(hash_base58_str.parse::<Hash>(), Ok(hash));

        // throw some non-base58 stuff in there
        hash_base58_str.replace_range(..1, "I");
        assert_eq!(
            hash_base58_str.parse::<Hash>(),
            Err(ParseHashError::Invalid)
        );
    }

}
