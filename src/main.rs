extern crate memmap2;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};

const INPUT_FILE_PATH: &str = "test/whole/ex.csv";
const OUTPUT_FILE_DIR: &str = "./test/parts/";

fn copy_csv(input_file: File, output_file: File) -> Result<usize, &'static str> {
    //let reader = BufReader::new(input_file);
    // TODO: these results need to be handled.
    let input_mmap: Mmap = unsafe { Mmap::map(&input_file).unwrap() }; // unsafe due to external file changes exploding it.

    let content: &[u8] = &input_mmap[..];
    let content_size = content.len();

    let io_size_result = output_file.set_len((content_size) as u64);
    if io_size_result.is_err() {
        return Err("Failed to set length of output file.");
    }

    //let mut output_mmap: MmapMut = unsafe { MmapMut::map_mut(&output_file).unwrap() }; // unsafe due to external file changes exploding it.
    let mut output_mmap = unsafe { MmapOptions::new().map_mut(&output_file).unwrap() };

    //unsafe {
    //    std::ptr::copy_nonoverlapping(input_mmap.as_ptr(), output_mmap.as_mut_ptr(), content_size);
    //}

    unsafe {
        copy_csv_line_by_line(input_mmap, &output_mmap);
    }

    // Explicitly flush to ensure data is written
    let result_ = output_mmap.flush();

    if result_.is_err() {
        return Err("Failed to flush mmap");
    }

    return Ok(content_size);
}

unsafe fn copy_csv_line_by_line(input_mmap: Mmap, output_mmap: &MmapMut) {
    // scan for newline
    let content: &[u8] = &input_mmap[..];
    let content_size = content.len();

    let input_ptr: *const u8 = input_mmap.as_ptr();
    let output_ptr: *mut u8 = output_mmap.as_mut_ptr();

    let mut count: usize = 0;
    let mut lastLine: usize = 0;

    while count < content_size {
        // scan for \n

        if (content[count] == b'\n') {
            // copy
            let copy_size = count - lastLine;
            std::ptr::copy_nonoverlapping(
                input_ptr.wrapping_add(lastLine),
                output_ptr.wrapping_add(lastLine),
                copy_size,
            );
        }

        count += 1;
        lastLine = count;
    }


}

fn main() {
    println!("{}", INPUT_FILE_PATH);
    println!("{}", OUTPUT_FILE_DIR);

    let res = File::open(INPUT_FILE_PATH);
    if !res.is_ok() {
        return;
    }
    let input_file: File = res.unwrap();
    let mut output_file_path: String = String::from(OUTPUT_FILE_DIR);
    output_file_path.push_str("output.csv");
    //let res = File::create(output_file_path.clone());

    let res = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(output_file_path.clone());

    if !res.is_ok() {
        println!("Create output file failure {}", output_file_path.clone());
        return;
    }
    let output_file: File = res.unwrap();

    let copy_result = copy_csv(input_file, output_file);

    if copy_result.is_ok() {
        println!("Copy Success! bytes: {}", copy_result.unwrap());
    } else {
        println!("Copy failure! reason: {}", copy_result.unwrap());
    }
}
