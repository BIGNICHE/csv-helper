extern crate memmap2;
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File};

pub struct GrowingFile {
    current_capacity: usize,
    file: File,
    current_size: usize,
    mmap: MmapMut,
}

impl GrowingFile {
    pub fn new(file: File, initial_capacity: u64) -> Result<GrowingFile, std::io::Error> {
        file.set_len(initial_capacity)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        return Ok(Self {
            current_capacity: initial_capacity as usize,
            file: file,
            current_size: 0,
            mmap,
        });
    }

    pub fn write_n_from_ptr(&mut self, src:  usize, len: usize) -> std::io::Result<usize> {
        // Grow if needed
        if self.current_size + len >= self.current_capacity {
            self.grow(self.current_size + len)?;
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                src as *mut u8,
                self.mmap.as_mut_ptr().wrapping_add(self.current_size),
                len,
            )
        };
        self.current_size += len;

        return Ok(len);
    }

    fn grow(&mut self, requested_size: usize) -> std::io::Result<usize> {
        let mut new_size: usize = self.current_capacity;
        // double until container can store it.
        while new_size <= requested_size {
            new_size = new_size * 2;
        }

        // commit any changes from mmap.
        self.mmap.flush().unwrap();
        // grow file.
        self.file.set_len(new_size as u64)?;
        self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
        self.current_capacity = new_size;

        return Ok(new_size);
    }
    // finalize the file
    pub fn close(self) -> std::io::Result<usize> {
        self.mmap.flush().unwrap();
        drop(self.mmap);
        // shrink the file to fit its data.
        self.file.set_len(self.current_size as u64).unwrap();
        return Ok(self.current_size);
    }
}
/*
impl Drop for GrowingFile {
    fn drop(&mut self) {
        self.close().unwrap();
    }
}

     */
