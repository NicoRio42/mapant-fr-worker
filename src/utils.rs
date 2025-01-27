use log::{error, info};
use reqwest::blocking::{multipart, Client};
use reqwest::header::HeaderMap;
use std::fs::{read, File};
use std::io::{self};
use std::time::Instant;
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
        error!(
            "Failed to download file with url {}. Status: {}. Response: {:?}",
            response.status(),
            file_url,
            response.text()
        );

        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to download file.",
        )));
    }

    let mut file = File::create(file_path)?;
    copy(&mut response, &mut file)?;

    return Ok(());
}

pub fn upload_file(
    worker_id: &str,
    token: &str,
    url: String,
    origin: &str,
    file_name: String,
    file_path: std::path::PathBuf,
    mime_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Uploading file {}", &file_name);
    let start = Instant::now();

    let client = Client::new();
    let file = read(&file_path)?;

    let part = multipart::Part::bytes(file)
        .file_name(file_name.clone())
        .mime_str(mime_str)?;

    let form = multipart::Form::new().part("file", part);

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .header("Origin", origin)
        .multipart(form)
        .send()?;

    if response.status().is_success() {
        let duration = start.elapsed();

        info!("File {} uploaded in {:.1?}", &file_name, duration);
    } else {
        error!(
            "Failed to upload file {}: {} {}",
            &file_name,
            response.status(),
            response.text()?
        );
    }

    Ok(())
}

pub fn upload_files(
    worker_id: &str,
    token: &str,
    url: String,
    origin: &str,
    files: Vec<(String, String, PathBuf, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_names = files
        .iter()
        .map(|file| file.0.clone())
        .collect::<Vec<String>>()
        .join(" ");

    info!("Uploading files {}", &file_names);
    let start = Instant::now();

    let client = Client::new();
    let mut form = multipart::Form::new();

    for (file_name, file_formpart_name, file_path, mime_str) in files {
        let file = read(&file_path)?;

        let part = multipart::Part::bytes(file)
            .file_name(file_name.clone())
            .mime_str(&mime_str)?;

        form = form.part(file_formpart_name, part);
    }

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .header("Origin", origin)
        .multipart(form)
        .send()?;

    if response.status().is_success() {
        let duration = start.elapsed();

        info!("Files {} uploaded in {:.1?}", &file_names, duration);
    } else {
        error!(
            "Failed to upload files {}: {} {}",
            &file_names,
            response.status(),
            response.text()?
        );
    }

    Ok(())
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
    let tar_xz_file = File::open(input_file)?;
    let bz_decoder = XzDecoder::new(tar_xz_file);
    let mut archive = Archive::new(bz_decoder);
    archive.unpack(output_dir)?;

    Ok(())
}
