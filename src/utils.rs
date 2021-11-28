pub fn u32_from_le_bytes(b: &[u8]) -> u32 {
    let mut d : u32 = 0;
    for i in 0..4 {
        d |= (b[i] as u32) << i*8
    }

    d
}
