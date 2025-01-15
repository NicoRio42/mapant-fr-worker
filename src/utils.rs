use bzip2::write::BzEncoder;
use bzip2::Compression;
use reqwest::blocking::get;
use std::fs::File;
use std::io::{self};
use std::{io::copy, path::PathBuf};
use tar::Builder;

pub fn download_file(
    file_url: &str,
    file_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut response = get(file_url)?;
    let mut file = File::create(file_path)?;
    copy(&mut response, &mut file)?;

    return Ok(());
}

pub fn compress_directory(input_dir: &PathBuf, output_file: &PathBuf) -> io::Result<()> {
    // Create the output file
    let tar_bz2_file = File::create(output_file)?;

    // Wrap it in a BzEncoder for bzip2 compression
    let bz_encoder = BzEncoder::new(tar_bz2_file, Compression::best());

    // Create a tar archive and write files from the directory into it
    let mut tar_builder = Builder::new(bz_encoder);
    tar_builder.append_dir_all(".", input_dir)?;

    // Finish writing to ensure all data is flushed to the file
    tar_builder.finish()?;

    Ok(())
}
