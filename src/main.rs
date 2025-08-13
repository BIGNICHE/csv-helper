mod growing_file;

extern crate memmap2;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::io;
use std::ptr::slice_from_raw_parts;
use std::slice;
use std::str::from_utf8;
use std::sync::Mutex;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    time::Instant,
};

use growing_file::GrowingFile;

//const INPUT_FILE_PATH: &str = "test/whole/Paterson_Regional_A1_MagLev_DC_r1_voxet.csv";
const INPUT_FILE_PATH: &str = "test/whole/ex.csv";
const OUTPUT_FILE_DIR: &str = "./test/parts/";

struct FileCSVRow<T> {
    head: *const u8,
    len: usize,
    index_value: T,
}

struct OutputCSVFile {
    head: Mutex<*mut u8>,
    rows: Vec<FileCSVRow<u32>>,
    len: usize,
}

fn copy_csv_only_column(
    input_file: File,
    output_directory: &str,
    column_index: u32,
) -> io::Result<usize> {
    let input_mmap: Mmap = unsafe { Mmap::map(&input_file).unwrap() };
    let content: &[u8] = &input_mmap[..];
    let content_size = content.len();
    let input_ptr: *const u8 = input_mmap.as_ptr();

    // reading process can be chunked here.
    // right now single thread.
    let mut count: usize = 0;
    let mut current_column_idx: u32 = 0;

    //let mut index_head_ptr: *const u8 = input_ptr;
    let mut index_head_count: usize = 0;

    // read the first row, must be the header row.




    index_head_count = 0;

    let mut csv_rows: Vec<FileCSVRow<u32>> = Vec::new();
    let mut current_row: FileCSVRow<u32> = FileCSVRow {
        head: (input_ptr),
        len: 0,
        index_value: (0),
    };

    let mut row_start_count: usize = count;

    let indices_map: HashMap<u32, OutputCSVFile> = HashMap::new();

    let mut header_row: bool = true;

    while count < content_size {
        // scan for \n
        // and idx column.

        if ((content[count] == b',') || (content[count] == b'\n')) && !header_row {
            if current_column_idx == column_index {
                let index_bytes = &content[index_head_count..count];
                let index_utf8: &str = from_utf8(index_bytes).unwrap();
                let index_int: u32 = index_utf8.parse().expect("parser failure");
                current_row.index_value = index_int;
            }

            current_column_idx += 1;

            if current_column_idx == column_index {
                index_head_count = count;
            }
        }

        if (content[count] == b'\n') || (count == content_size - 1) { // count == content_size checks for eof
            // newline
            current_row.len = count - row_start_count;

            if header_row {
                header_row  = false;
            }

            csv_rows.push(current_row);
            current_row = FileCSVRow {
                head: (input_ptr.wrapping_add(count + 1)),
                len: 0,
                index_value: (0),
            };
            index_head_count = count + 1;
            current_column_idx = 0;
            row_start_count = count;
        }

        count += 1;
    }

    // now we have all the meta required to copy these rows out.

    let mut output_file_path = String::from(OUTPUT_FILE_DIR);
    output_file_path.push_str("output.csv");

    let res = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(output_file_path.clone());

    if !res.is_ok() {
        println!("Create output file failure {}", output_file_path.clone());
        return Err(res.unwrap_err());
    }

    let output_file = res.unwrap();

    let mut output_growing_file = GrowingFile::new(output_file, 1638400)?;

    //let header_slice = input_mmap.get(0..header_row.len).unwrap(); // TODO: handle this possible error
    /*
    let mut input_idx: usize = 0;
    input_idx += output_growing_file
        .write_n_from_ptr(input_ptr as *mut u8, header_row.len)
        .unwrap();
    */
    for row in csv_rows {
        output_growing_file
            .write_n_from_ptr(row.head as *mut u8, row.len)
            .unwrap();

        //input_idx += row.len;

        //let data = input_mmap.get(input_idx..input_idx+row.len).unwrap(); // TODO handle option
        //input_idx += output_growing_file.write_n_from_slice(data).unwrap();
    }


    /*
    output_file.set_len(count as u64)?;

    let mut output_mmap = MmapOptions::new().map_mut(&output_file).unwrap();

    // write header row

    let mut output_head = output_mmap.as_mut_ptr();

    std::ptr::copy_nonoverlapping(input_ptr, output_head, header_row.len);
    output_head = output_head.wrapping_add(header_row.len);
    let mut input_count = header_row.len;

    for row in csv_rows {
        std::ptr::copy_nonoverlapping(input_ptr.wrapping_add(input_count), output_head, row.len);
        input_count += row.len;
        output_head = output_head.wrapping_add(row.len);
    }
     */

    //return Ok(csv_rows.len() * std::mem::size_of::<FileCSVRow<u32>>());
    return Ok(output_growing_file.close().unwrap());
}

fn main() {
    let now = Instant::now();

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

    //let copy_result = copy_csv(input_file, output_file);
    let output_directory = "./test/parts/";
    let column_index = 0;

    let copy_result = unsafe { copy_csv_only_column(input_file, output_directory, column_index) };

    let elapsed_time = now.elapsed();

    if copy_result.is_ok() {
        println!(
            "Copy Success! bytes: {}, in {} ms",
            copy_result.unwrap(),
            elapsed_time.as_millis()
        );
    } else {
        println!(
            "Copy failure! reason: {}, in {} ms",
            copy_result.unwrap(),
            elapsed_time.as_millis()
        );
    }
}
