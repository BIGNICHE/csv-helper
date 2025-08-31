mod growing_file;

extern crate memmap2;
use growing_file::GrowingFile;
use memmap2::Mmap;
use std::io;
use std::str::from_utf8;
use std::sync::Mutex;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    sync::Arc,
    thread,
    time::Instant,
};

const INPUT_FILE_PATH: &str = "test/whole/ex_large.csv";
const OUTPUT_FILE_DIR: &str = "./test/parts/";

#[derive(Clone)]
struct FileCSVRow<T> {
    head: usize,
    len: usize,
    index_value: T,
}

fn copy_csv_only_column(
    input_file: File,
    input_name: &str,
    output_directory: &str,
    column_index: u32,
) -> io::Result<usize> {
    let input_mmap: Mmap = unsafe { Mmap::map(&input_file)? };
    let content: &[u8] = &input_mmap[..];
    let content_size = content.len();
    let input_ptr: *const u8 = input_mmap.as_ptr();

    let (header_row, indices_map) =
        create_file_index(column_index, &content, content_size, input_ptr);

    let iter_indices = indices_map.into_iter();

    let output_size_guess: usize = content_size / iter_indices.len(); // Initial file size for the output growing files.

    let arc_iter = Arc::new(Mutex::new(iter_indices));

    let mut workers = vec![];

    for _i in 0..24 {
        // copy setup required
        let iterator = Arc::clone(&arc_iter);
        let iname = input_name.to_string();
        let ioutput_dir = output_directory.to_string();
        let header_copy = header_row.clone();
        workers.push(thread::spawn(move || {
            loop {
                let nxt = iterator.lock().unwrap().next(); // should unblock
                if nxt.is_none() {
                    return;
                }
                let index_tuple = nxt.unwrap();
                // make the file.
                let index = index_tuple.0;
                let output_file_path = String::from(&format!(
                    "{}{}_IL{}.csv",
                    ioutput_dir,
                    iname,
                    index.to_string()
                ));
                let output_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&output_file_path).unwrap();

                let mut output_growing_file =
                    GrowingFile::new(output_file, output_size_guess.clone() as u64).unwrap();
                output_growing_file
                    .write_n_from_ptr(header_copy.head, header_copy.len)
                    .unwrap();

                for row in index_tuple.1 {
                    output_growing_file
                        .write_n_from_ptr(row.head, row.len)
                        .unwrap();
                }
                output_growing_file.close().unwrap();
            }
        }));
    }

    for handle in workers {
        handle.join().unwrap();
    }

    /*
    for row in csv_rows {
        output_growing_file
            .write_n_from_ptr(row.head as *mut u8, row.len)
            .unwrap();
    }
     */

    Ok(content_size)
}

fn create_file_index(
    column_index: u32,
    content: &&[u8],
    content_size: usize,
    input_ptr: *const u8,
) -> (FileCSVRow<u32>, HashMap<u32, Vec<FileCSVRow<u32>>>) {
    let mut indices_map: HashMap<u32, Vec<FileCSVRow<u32>>> = HashMap::new();
    let mut current_row: FileCSVRow<u32> = FileCSVRow {
        head: input_ptr as usize,
        len: 0,
        index_value: 0,
    };
    let mut header_row: FileCSVRow<u32> = FileCSVRow {
        head: input_ptr as usize,
        len: 0,
        index_value: 0,
    };
    let mut count: usize = 0;
    let mut current_column_idx = 0;
    let mut index_head_count = 0;
    let mut is_header_row = true;
    let mut row_start_count = 0;
    while count < content_size {
        // scan for \n
        // and idx column.

        if ((content[count] == b',') || (content[count] == b'\n')) && !is_header_row {
            if current_column_idx == column_index {
                let index_bytes = &content[index_head_count..count];
                let index_utf8: &str = from_utf8(index_bytes).unwrap();
                let index_int: u32 = index_utf8.parse().expect("parser failure");
                current_row.index_value = index_int;
            }

            current_column_idx += 1;

            if current_column_idx == column_index {
                index_head_count = count + 1; // Add 1 for the comma.
            }
        }

        if (content[count] == b'\n') || (count == content_size - 1) {
            // count == content_size checks for eof
            // newline
            current_row.len = count - row_start_count;

            if is_header_row {
                is_header_row = false;
                header_row = current_row.clone();
            } else if indices_map.contains_key(&current_row.index_value) {
                indices_map
                    .get_mut(&current_row.index_value)
                    .unwrap()
                    .push(current_row);
            } else {
                indices_map.insert(current_row.index_value, vec![current_row]);
            }
            current_row = FileCSVRow {
                head: input_ptr.wrapping_add(count + 1) as usize,
                len: 0,
                index_value: 0,
            };
            index_head_count = count + 1;
            current_column_idx = 0;
            row_start_count = count;
        }

        count += 1;
    }

    (header_row, indices_map)
}

fn main() {
    let now = Instant::now();

    let res = File::open(INPUT_FILE_PATH);
    if !res.is_ok() {
        return;
    }
    let input_file: File = res.unwrap();

    let output_directory = OUTPUT_FILE_DIR;
    let column_index = 0;

    let copy_result = copy_csv_only_column(
        input_file,
        "Paterson_Regional_A1_MagLev_DC_r1_voxet",
        output_directory,
        column_index,
    );

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
