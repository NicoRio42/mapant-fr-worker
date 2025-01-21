use std::{
    fs::{create_dir_all, read},
    path::{Path, PathBuf},
};

use cassini::process_single_tile_render_step;
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

    if !lidar_step_tile_dir_path.exists() {
        create_dir_all(&lidar_step_tile_dir_path)?;

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

        decompress_archive(&lidar_step_archive_path, &lidar_step_tile_dir_path)?;
    };

    let mut neighbor_tiles_lidar_step_dir_paths: Vec<PathBuf> = vec![];

    // Downloading lidar step files for the neigbhoring tiles if not already on disk
    for neigbhoring_tile_id in neigbhoring_tiles_ids {
        let neigbhoring_tile_lidar_step_dir_path =
            lidar_step_base_dir_path.join(neigbhoring_tile_id);

        if !neigbhoring_tile_lidar_step_dir_path.exists() {
            create_dir_all(&neigbhoring_tile_lidar_step_dir_path)?;

            let neigbhoring_tile_lidar_step_archive_url = format!(
                "{}/api/map-generation/lidar-steps/{}",
                base_api_url, neigbhoring_tile_id
            );

            let neigbhoring_tile_lidar_step_archive_path =
                lidar_step_base_dir_path.join(format!("{}.tar.bz2", neigbhoring_tile_id));

            let mut headers = HeaderMap::new();

            headers.append(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {}.{}", worker_id, token))?,
            );

            download_file(
                &neigbhoring_tile_lidar_step_archive_url,
                &neigbhoring_tile_lidar_step_archive_path,
                Some(headers),
            )?;

            decompress_archive(
                &neigbhoring_tile_lidar_step_archive_path,
                &neigbhoring_tile_lidar_step_dir_path,
            )?;
        };

        neighbor_tiles_lidar_step_dir_paths.push(neigbhoring_tile_lidar_step_dir_path);
    }

    let render_step_path = Path::new("render-step");

    if !render_step_path.exists() {
        create_dir_all(render_step_path)?;
    }

    let output_dir_path = render_step_path.join(&tile_id);

    process_single_tile_render_step(
        &lidar_step_tile_dir_path,
        &output_dir_path,
        neighbor_tiles_lidar_step_dir_paths,
        false,
    );

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
        println!("File uploaded successfully: {}", response.text()?);
    } else {
        println!(
            "Failed to upload file: {} {}",
            response.status(),
            response.text()?
        )
    }

    Ok(())
}
