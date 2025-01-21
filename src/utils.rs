use reqwest::blocking::Client;
use reqwest::header::HeaderMap;
use std::fs::File;
use std::io::{self};
use std::{io::copy, path::PathBuf};
use tar::Archive;
use tar::Builder;
use xz2::read::XzDecoder;
use xz2::write::XzEncoder;

pub fn download_file(
    file_url: &str,
    file_path: &PathBuf,
    headers: Option<HeaderMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();

    let request = match headers {
        Some(h) => client.get(file_url).headers(h),
        None => client.get(file_url),
    };

    let mut response = request.send()?;

    if !response.status().is_success() {
        println!("Failed to download file: {}", response.status());
    }

    let mut file = File::create(file_path)?;
    copy(&mut response, &mut file)?;

    return Ok(());
}

pub fn compress_directory(input_dir: &PathBuf, output_file: &PathBuf) -> io::Result<()> {
    let tar_xz_file = File::create(output_file)?;
    let xz_encoder = XzEncoder::new(tar_xz_file, 6);
    let mut tar_builder = Builder::new(xz_encoder);
    tar_builder.append_dir_all(".", input_dir)?;
    tar_builder.finish()?;

    Ok(())
}

pub fn decompress_archive(input_file: &PathBuf, output_dir: &PathBuf) -> io::Result<()> {
    let tar_bz2_file = File::open(input_file)?;
    let bz_decoder = XzDecoder::new(tar_bz2_file);
    let mut archive = Archive::new(bz_decoder);
    archive.unpack(output_dir)?;

    Ok(())
}
