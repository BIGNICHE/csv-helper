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

    //unsafe {
    //    copy_csv_line_by_line(input_mmap, &mut output_mmap);
    //}

    // Explicitly flush to ensure data is written
    let result_ = output_mmap.flush();

    if result_.is_err() {
        return Err("Failed to flush mmap");
    }

    return Ok(content_size);
}

unsafe fn copy_csv_line_by_line(input_mmap: Mmap, output_mmap: &mut MmapMut) {
    // scan for newline
    let content: &[u8] = &input_mmap[..];
    let content_size = content.len();

    let input_ptr: *const u8 = input_mmap.as_ptr();
    let output_ptr: *mut u8 = output_mmap.as_mut_ptr();

    let mut count: usize = 0;
    let mut last_line: usize = 0;

    while count < content_size {
        // scan for \n

        if content[count] == b'\n' {
            // copy
            let copy_size = count - last_line + 1;
            std::ptr::copy_nonoverlapping(
                input_ptr.wrapping_add(last_line),
                output_ptr.wrapping_add(last_line),
                copy_size,
            );
            last_line = count + 1;
        }

        count += 1;
    }
}

// will open the file in read-write mode.
unsafe fn get_file_buffer_from_name(input_file_name: &str) -> io::Result<*mut u8> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(input_file_name)?;
    return get_file_buffer(file);
}

// treats the buffer as mutable.
unsafe fn get_file_buffer(input_file: File) -> io::Result<*mut u8> {
    let mut file_mmap = MmapOptions::new().map_mut(&input_file)?;
    return Ok(file_mmap.as_mut_ptr());
}