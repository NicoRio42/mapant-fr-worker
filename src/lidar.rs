use cassini::process_single_tile_lidar_step;
use std::{fs::create_dir_all, path::Path};

use crate::utils::{compress_directory, download_file};

pub fn lidar_step(x: i32, y: i32, tile_url: String) -> Result<(), Box<dyn std::error::Error>> {
    let lidar_files_path = Path::new("lidar-files");
    let lidar_file_path = lidar_files_path.join(format!("{}_{}.laz", x, y));

    if !lidar_files_path.exists() {
        create_dir_all(lidar_files_path)?;
    }

    download_file(&tile_url, &lidar_file_path)?;

    let lidar_step_path = Path::new("lidar-step");

    if !lidar_step_path.exists() {
        create_dir_all(lidar_step_path)?;
    }

    let output_dir_path = lidar_step_path.join(format!("{}_{}", x, y));
    process_single_tile_lidar_step(&lidar_file_path, &output_dir_path);
    let archive_path = lidar_step_path.join(format!("{}_{}.tar.bz2", x, y));
    compress_directory(&output_dir_path, &archive_path)?;

    return Ok(());
}
