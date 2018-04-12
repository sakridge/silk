use std::os::raw::c_char;

// k = number of data devices
// m = number of coding devices
// w = word size

extern "C" {
    fn jerasure_matrix_encode(k: i32, m: i32, w: i32,
                              matrix: *const i32,
                              data_ptrs: *const *const u8,
                              coding_ptrs: *const *const u8,
                              size: i32);
    fn jerasure_matrix_decode(k: i32, m: i32, w: i32,
                              matrix: *const i32,
                              row_k_ones: i32,
                              erasures: *const i32,
                              data_ptrs: *const *const u8,
                              coding_ptrs: *const *const u8,
                              size: i32) -> i32;
    fn galois_single_divide(a: i32, b: i32, w: i32) -> i32;
}

fn get_matrix(m: i32, k: i32, w: i32) -> Vec<i32> {
    let mut matrix = vec![0; (m * k) as usize];
    for i in 0..m {
        for j in 0..k {
            unsafe {
                matrix[(i*k+j) as usize] = galois_single_divide(1, i ^ (m + j), w);
            }
        }
    }
    matrix
}

pub const ERASURE_W: i32 = 32;

pub fn generate_coding_blocks(data: &[Vec<u8>], m: i32) -> Vec<Vec<u8>> {
    let matrix: Vec<i32> = get_matrix(m, data.len() as i32, ERASURE_W);
    let mut vs = Vec::new();
    let mut coding_arg: Vec<*const u8> = Vec::new();
    let mut data_arg = Vec::new();
    for _ in 0..m {
        let v = vec![0; data[0].len()];
        vs.push(v);
    }
    println!("{:?}", vs);
    for mut x in vs.iter_mut() {
        coding_arg.push(x.as_ptr());
    }
    for block in data {
        data_arg.push(block.as_ptr());
    }
    unsafe {
        jerasure_matrix_encode(data.len() as i32,
                               m,
                               ERASURE_W,
                               matrix.as_ptr(),
                               data_arg.as_ptr(),
                               coding_arg.as_ptr(),
                               data[0].len() as i32);
    }
    vs
}

pub fn decode_blocks(data: &[Vec<u8>], coding: &[Vec<u8>], erasures: &[i32]) {
    let matrix: Vec<i32> = get_matrix(coding.len() as i32, data.len() as i32, ERASURE_W);
    let mut coding_arg: Vec<*const u8> = Vec::new();
    let mut data_arg: Vec<*const u8> = Vec::new();
    for x in coding.iter() {
        coding_arg.push(x.as_ptr());
    }
    for x in data.iter() {
        data_arg.push(x.as_ptr());
    }
    unsafe {
        let ret = jerasure_matrix_decode(data.len() as i32,
                                         coding.len() as i32,
                                         ERASURE_W,
                                         matrix.as_ptr(),
                                         0,
                                         erasures.as_ptr(),
                                         data_arg.as_ptr(),
                                         coding_arg.as_ptr(),
                                         data[0].len() as i32);
        println!("ret: {}", ret);
    }
}

#[cfg(test)]
mod test {
    use erasure;

    #[test]
    pub fn coding_test() {
        let mut vs = Vec::new();
        let v_orig = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        for i in 0..4 {
            let mut v = v_orig.clone();
            for mut x in v.iter_mut() {
                *x += i;
            }
            vs.push(v);
        }

        let m = 2;
        let coding_blocks = erasure::generate_coding_blocks(vs.as_slice(),
                                                            m);
        for v in coding_blocks.iter() {
            println!("{:?}", v);
        }
        let erasure: i32 = 1;
        let erasures = vec![erasure, -1];
        // clear an entry
        vs[erasure as usize] = vec![0; 16];
        erasure::decode_blocks(vs.as_slice(),
                               coding_blocks.as_slice(),
                               erasures.as_slice());
        println!("vs:");
        for v in vs.iter() {
            println!("{:?}", v);
        }
        assert_eq!(v_orig, vs[0]);
    }
}
