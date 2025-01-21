use cassini::process_single_tile_lidar_step;
use reqwest::blocking::{multipart, Client};
use std::fs::read;
use std::{fs::create_dir_all, path::Path};

use crate::utils::{compress_directory, download_file};

pub fn lidar_step(
    tile_id: &str,
    laz_file_url: &str,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lidar_files_path = Path::new("lidar-files");
    let lidar_file_path = lidar_files_path.join(format!("{}.laz", &tile_id));

    if !lidar_files_path.exists() {
        create_dir_all(lidar_files_path)?;
    }

    println!("Downloading {}", &laz_file_url);
    download_file(&laz_file_url, &lidar_file_path, None)?;

    let lidar_step_path = Path::new("lidar-step");

    if !lidar_step_path.exists() {
        create_dir_all(lidar_step_path)?;
    }

    let output_dir_path = lidar_step_path.join(&tile_id);
    process_single_tile_lidar_step(&lidar_file_path, &output_dir_path);
    let archive_file_name = format!("{}.tar.xz", &tile_id);
    let archive_path = lidar_step_path.join(&archive_file_name);
    compress_directory(&output_dir_path, &archive_path)?;

    let client = Client::new();
    let file = read(&archive_path)?;

    let part = multipart::Part::bytes(file)
        .file_name(archive_file_name.clone())
        .mime_str("application/x-bzip2")?;

    let form = multipart::Form::new().part("file", part);

    let url = format!(
        "{}/api/map-generation/lidar-steps/{}",
        base_api_url, &tile_id
    );

    println!("{}", url);

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .header("Origin", base_api_url)
        .multipart(form)
        .send()?;

    if response.status().is_success() {
        println!("File uploaded successfully: {}", response.text()?);
    } else {
        println!(
            "Failed to upload file: {} {}",
            response.status(),
            response.text()?
        );
    }

    return Ok(());
}
