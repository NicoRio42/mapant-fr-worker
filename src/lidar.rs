use cassini::process_single_tile_lidar_step;
use reqwest::blocking::{get, multipart, Client};
use std::fs::{read, File};
use std::{fs::create_dir_all, path::Path};

use crate::utils::{compress_directory, download_file};

pub fn lidar_step(
    x: i32,
    y: i32,
    tile_url: String,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lidar_files_path = Path::new("lidar-files");
    let tile_id = format!("{}_{}", x, y);
    let lidar_file_path = lidar_files_path.join(format!("{}.laz", &tile_id));

    if !lidar_files_path.exists() {
        create_dir_all(lidar_files_path)?;
    }

    download_file(&tile_url, &lidar_file_path)?;

    let lidar_step_path = Path::new("lidar-step");

    if !lidar_step_path.exists() {
        create_dir_all(lidar_step_path)?;
    }

    let output_dir_path = lidar_step_path.join(&tile_id);
    process_single_tile_lidar_step(&lidar_file_path, &output_dir_path);
    let archive_file_name = format!("{}.tar.bz2", &tile_id);
    let archive_path = lidar_step_path.join(&archive_file_name);
    compress_directory(&output_dir_path, &archive_path)?;

    let client = Client::new();
    let file = read(&archive_path)?;

    let part = multipart::Part::bytes(file)
        .file_name(archive_file_name.clone())
        .mime_str("application/x-bzip2")?;

    let form = multipart::Form::new().part("file", part);
    let url = format!("{}/map-generation/lidar-steps/{}", base_api_url, &tile_id);

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .multipart(form)
        .send()?;

    if response.status().is_success() {
        println!("File uploaded successfully: {}", response.text()?);
    } else {
        println!("Failed to upload file: {}", response.status());
    }

    return Ok(());
}
