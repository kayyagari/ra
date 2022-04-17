use crc32fast::Hasher;

pub mod test_utils;

pub mod bson_utils;
pub mod resources;
pub mod validator;

pub fn u32_from_le_bytes(b: &[u8]) -> u32 {
    let mut d : u32 = 0;
    for i in 0..4 {
        d |= (b[i] as u32) << i*8
    }

    d
}

pub fn get_crc_hash<S: AsRef<str>>(k: S) -> [u8;4] {
    let mut hasher = Hasher::new();
    hasher.update(k.as_ref().as_bytes());
    let i = hasher.finalize();
    i.to_le_bytes()
}

pub fn prefix_id(prefix: &[u8], ksid: &[u8]) -> [u8; 24]{
    let mut tmp: [u8; 24] = [0; 24];
    tmp[..4].copy_from_slice(prefix);
    tmp[4..].copy_from_slice(ksid);

    tmp
}
