use solana_sdk::timing::duration_as_ns;
use std::time::Instant;

#[cfg(feature = "nvtx")]
#[link(name = "nvToolsExt")]
extern "C" {
    fn nvtxRangeStartA(name: *const libc::c_char) -> u64;
    fn nvtxRangeEnd(id: u64);
}

#[cfg(feature = "nvtx")]
pub fn nv_range_start(name: String) -> u64 {
    use std::ffi::CString;

    unsafe {
        let cname = CString::new(name).unwrap();
        nvtxRangeStartA(cname.as_ptr())
    }
}

#[cfg(feature = "nvtx")]
pub fn nv_range_end(id: u64) {
    unsafe {
        nvtxRangeEnd(id);
    }
}

pub struct Measure {
    start: Instant,
    duration: u64,
    #[cfg(feature = "nvtx")]
    nvtx_id: u64,
}

impl Measure {
    pub fn start(_name: &'static str) -> Self {
        #[cfg(feature = "nvtx")]
        let nvtx_id = nv_range_start(_name.to_string());
        Self {
            start: Instant::now(),
            duration: 0,
            #[cfg(feature = "nvtx")]
            nvtx_id,
        }
    }

    pub fn stop(&mut self) {
        #[cfg(feature = "nvtx")]
        nv_range_end(self.nvtx_id);
        self.duration = duration_as_ns(&self.start.elapsed());
    }

    pub fn as_us(&self) -> u64 {
        self.duration / 1000
    }

    pub fn as_ms(&self) -> u64 {
        self.duration / (1000 * 1000)
    }

    pub fn as_s(&self) -> f32 {
        self.duration as f32 / (1000.0f32 * 1000.0f32 * 1000.0f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_measure() {
        let mut measure = Measure::start("test");
        sleep(Duration::from_secs(1));
        measure.stop();
        assert!(measure.as_s() >= 0.99f32 && measure.as_s() <= 1.01f32);
        assert!(measure.as_ms() >= 990 && measure.as_ms() <= 1_010);
        assert!(measure.as_us() >= 999_000 && measure.as_us() <= 1_010_000);
    }
}
