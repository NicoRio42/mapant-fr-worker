use std::{
    fs::{create_dir_all, read},
    path::{Path, PathBuf},
    time::Instant,
};

use cassini::process_single_tile_render_step;
use log::{error, info};
use reqwest::{
    blocking::{multipart, Client},
    header::{HeaderMap, HeaderValue},
};

use crate::utils::{decompress_archive, download_file};

pub fn render_step(
    tile_id: &str,
    neigbhoring_tiles_ids: &Vec<String>,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lidar_step_base_dir_path = Path::new("lidar-step");

    if !lidar_step_base_dir_path.exists() {
        create_dir_all(lidar_step_base_dir_path)?;
    }

    // Downloading lidar step files for the tile if not already on disk
    let lidar_step_tile_dir_path = lidar_step_base_dir_path.join(tile_id);

    download_and_decompress_lidar_step_files_if_not_on_disk(
        tile_id,
        worker_id,
        token,
        base_api_url,
        lidar_step_base_dir_path,
        &lidar_step_tile_dir_path,
    )?;

    let mut neighbor_tiles_lidar_step_dir_paths: Vec<PathBuf> = vec![];

    // Downloading lidar step files for the neigbhoring tiles if not already on disk
    for neigbhoring_tile_id in neigbhoring_tiles_ids {
        let neigbhoring_tile_lidar_step_dir_path =
            lidar_step_base_dir_path.join(neigbhoring_tile_id);

        download_and_decompress_lidar_step_files_if_not_on_disk(
            tile_id,
            worker_id,
            token,
            base_api_url,
            lidar_step_base_dir_path,
            &neigbhoring_tile_lidar_step_dir_path,
        )?;

        neighbor_tiles_lidar_step_dir_paths.push(neigbhoring_tile_lidar_step_dir_path);
    }

    let render_step_path = Path::new("render-step");

    if !render_step_path.exists() {
        create_dir_all(render_step_path)?;
    }

    let output_dir_path = render_step_path.join(&tile_id);

    info!("Processing render step for tile {}", &tile_id);
    let start = Instant::now();

    process_single_tile_render_step(
        &lidar_step_tile_dir_path,
        &output_dir_path,
        neighbor_tiles_lidar_step_dir_paths,
        false,
    );

    let duration = start.elapsed();

    info!(
        "Render step for tile {} processed in {:.1?}",
        &tile_id, duration
    );

    info!("Uploading resulting png map for tile {}", &tile_id);
    let start = Instant::now();

    let full_map_png_path = output_dir_path.join("full-map.png");

    let client = Client::new();
    let file = read(&full_map_png_path)?;

    let part = multipart::Part::bytes(file)
        .file_name(format!("{}.png", tile_id))
        .mime_str("image/png")?;

    let form = multipart::Form::new().part("file", part);
    let url = format!(
        "{}/api/map-generation/render-steps/{}",
        base_api_url, &tile_id
    );

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .header("Origin", base_api_url)
        .multipart(form)
        .send()?;

    if response.status().is_success() {
        let duration = start.elapsed();

        info!(
            "Resulting png map for tile {} uploaded in {:.1?}",
            &tile_id, duration
        );
    } else {
        error!(
            "Failed to upload resulting png map for tile {}: {} {}",
            &tile_id,
            response.status(),
            response.text()?
        );
    }

    Ok(())
}

fn download_and_decompress_lidar_step_files_if_not_on_disk(
    tile_id: &str,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
    lidar_step_base_dir_path: &Path,
    lidar_step_tile_dir_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    if !lidar_step_tile_dir_path.exists() {
        info!("Downloading files from LiDAR step for tile {}", &tile_id);
        let start = Instant::now();

        create_dir_all(lidar_step_tile_dir_path)?;

        let lidar_step_archive_url = format!(
            "{}/api/map-generation/lidar-steps/{}",
            base_api_url, tile_id
        );

        let lidar_step_archive_path = lidar_step_base_dir_path.join(format!("{}.tar.bz2", tile_id));

        let mut headers = HeaderMap::new();

        headers.append(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}.{}", worker_id, token))?,
        );

        download_file(
            &lidar_step_archive_url,
            &lidar_step_archive_path,
            Some(headers),
        )?;

        let duration = start.elapsed();

        info!(
            "Files from LiDAR step for tile {} downloaded in {:.1?}",
            &tile_id, duration
        );

        info!("Decompressing files from LiDAR step for tile {}", &tile_id);
        let start = Instant::now();

        decompress_archive(&lidar_step_archive_path, lidar_step_tile_dir_path)?;

        let duration = start.elapsed();

        info!(
            "Files from LiDAR step for tile {} decompressed in {:.1?}",
            &tile_id, duration
        );
    } else {
        info!(
            "Files from LiDAR step for tile {} already on disk.",
            &tile_id
        );
    };
    Ok(())
}
