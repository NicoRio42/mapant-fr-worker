use image::{imageops::FilterType, GenericImage, GenericImageView, Rgba, RgbaImage};
use log::{error, info};
use reqwest::{
    blocking::{multipart, Client},
    header::{HeaderMap, HeaderValue},
};
use std::{
    fs::{create_dir_all, read},
    path::{Path, PathBuf},
    time::Instant,
};

use crate::utils::download_file;

const TILE_PIXEL_SIZE: u32 = 256;

pub fn pyramid_step(
    x: i32,
    y: i32,
    z: i32,
    base_zoom_level_tile_id: Option<String>,
    area_id: String,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tiles_dir_path = Path::new("tiles");

    if !tiles_dir_path.exists() {
        create_dir_all(tiles_dir_path)?;
    }

    let area_tiles_dir_path = tiles_dir_path.join(&area_id);

    if !area_tiles_dir_path.exists() {
        create_dir_all(&area_tiles_dir_path)?;
    }

    match base_zoom_level_tile_id {
        Some(tile_id) => {
            pyramid_step_base_zoom_level(
                x,
                y,
                area_id,
                worker_id,
                token,
                base_api_url,
                &area_tiles_dir_path,
                tile_id,
            )?;
        }
        None => {
            pyramid_step_lower_zoom_level(
                x,
                y,
                z,
                area_id,
                worker_id,
                token,
                base_api_url,
                &area_tiles_dir_path,
            )?;
        }
    }

    Ok(())
}

pub fn pyramid_step_base_zoom_level(
    x: i32,
    y: i32,
    area_id: String,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
    area_tiles_dir_path: &PathBuf,
    tile_id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Downloading the base high quality tile for tile {}",
        &tile_id
    );

    let start = Instant::now();

    let zoom_11_x_path = area_tiles_dir_path.join("11").join(x.to_string());

    if !zoom_11_x_path.exists() {
        create_dir_all(&zoom_11_x_path)?;
    }

    let zoom_11_tile_path = zoom_11_x_path.join(format!("{}.png", y));

    let zoom_11_tile_url = format!(
        "{}/api/map-generation/render-steps/{}",
        base_api_url, tile_id
    );

    let mut headers = HeaderMap::new();

    headers.append(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}.{}", worker_id, token))?,
    );

    download_file(&zoom_11_tile_url, &zoom_11_tile_path, Some(headers))?;

    let duration = start.elapsed();

    info!(
        "Base high quality tile for tile {} downloaded in {:.1?}",
        &tile_id, duration
    );

    info!(
        "Generating tiles for zoom 11, 12 and 13 for high quality tile {}",
        &tile_id
    );

    let start = Instant::now();

    let zoom_12_path = &area_tiles_dir_path.join("12");
    let zoom_12_x_path = &zoom_12_path.join((x * 2).to_string());
    let zoom_12_x_plus_1_path = &zoom_12_path.join((x * 2 + 1).to_string());

    if !zoom_12_x_path.exists() {
        create_dir_all(zoom_12_x_path)?;
    }

    if !zoom_12_x_plus_1_path.exists() {
        create_dir_all(zoom_12_x_plus_1_path)?;
    }

    let zoom_12_tiles_paths = [
        &zoom_12_x_path.join(format!("{}.png", (y * 2).to_string())),
        &zoom_12_x_plus_1_path.join(format!("{}.png", (y * 2).to_string())),
        &zoom_12_x_path.join(format!("{}.png", (y * 2 + 1).to_string())),
        &zoom_12_x_plus_1_path.join(format!("{}.png", (y * 2 + 1).to_string())),
    ];

    split_image_in_four(&zoom_11_tile_path, &zoom_12_tiles_paths)?;

    // Generate tiles for zoom 13
    let zoom_12_tiles = [
        [x * 2, y * 2],
        [x * 2 + 1, y * 2],
        [x * 2, y * 2 + 1],
        [x * 2 + 1, y * 2 + 1],
    ];

    for (i_12, [x_12, y_12]) in zoom_12_tiles.iter().enumerate() {
        let zoom_13_path = &area_tiles_dir_path.join("13");
        let zoom_13_x_path = &zoom_13_path.join((x_12 * 2).to_string());
        let zoom_13_x_plus_1_path = &zoom_13_path.join((x_12 * 2 + 1).to_string());

        if !zoom_13_x_path.exists() {
            create_dir_all(zoom_13_x_path)?;
        }

        if !zoom_13_x_plus_1_path.exists() {
            create_dir_all(zoom_13_x_plus_1_path)?;
        }

        let zoom_13_tiles_paths = [
            &zoom_13_x_path.join(format!("{}.png", (y_12 * 2).to_string())),
            &zoom_13_x_plus_1_path.join(format!("{}.png", (y_12 * 2).to_string())),
            &zoom_13_x_path.join(format!("{}.png", (y_12 * 2 + 1).to_string())),
            &zoom_13_x_plus_1_path.join(format!("{}.png", (y_12 * 2 + 1).to_string())),
        ];

        split_image_in_four(&zoom_12_tiles_paths[i_12], &zoom_13_tiles_paths)?;

        // Resize and upload zoom 13 tiles
        let mut i_13 = 0;

        let zoom_13_tiles = [
            [x_12 * 2, y_12 * 2],
            [x_12 * 2 + 1, y_12 * 2],
            [x_12 * 2, y_12 * 2 + 1],
            [x_12 * 2 + 1, y_12 * 2 + 1],
        ];

        for zoom_13_tile_path in zoom_13_tiles_paths {
            resize_image_in_place(zoom_13_tile_path, TILE_PIXEL_SIZE, TILE_PIXEL_SIZE)?;
            let [x_13, y_13] = zoom_13_tiles[i_13];

            upload_tile(
                base_api_url,
                zoom_13_tile_path,
                format!("{}.png", y_13),
                &area_id,
                13,
                x_13,
                y_13,
                worker_id,
                token,
            )?;

            i_13 += 1;
        }
    }

    // Resize and upload zoom 12 tiles
    let mut i_12 = 0;

    for zoom_12_tile_path in zoom_12_tiles_paths {
        resize_image_in_place(zoom_12_tile_path, TILE_PIXEL_SIZE, TILE_PIXEL_SIZE)?;
        let [x_12, y_12] = zoom_12_tiles[i_12];

        upload_tile(
            base_api_url,
            zoom_12_tile_path,
            format!("{}.png", y_12),
            &area_id,
            12,
            x_12,
            y_12,
            worker_id,
            token,
        )?;

        i_12 += 1;
    }

    // Resize and upload zoom 11 tile
    resize_image_in_place(&zoom_11_tile_path, TILE_PIXEL_SIZE, TILE_PIXEL_SIZE)?;

    upload_tile(
        base_api_url,
        &zoom_11_tile_path,
        format!("{}.png", y),
        &area_id,
        11,
        x,
        y,
        worker_id,
        token,
    )?;

    let duration = start.elapsed();

    info!(
        "Tiles for zoom 11, 12 and 13 for high quality tile {} generated in {:.1?}",
        &tile_id, duration
    );

    Ok(())
}

pub fn pyramid_step_lower_zoom_level(
    x: i32,
    y: i32,
    z: i32,
    area_id: String,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
    area_tiles_dir_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Zoom={} x={} y={}, Trying to download children tiles",
        z, x, y
    );

    let start = Instant::now();

    let children_tiles = [
        [x * 2, y * 2],
        [x * 2 + 1, y * 2],
        [x * 2, y * 2 + 1],
        [x * 2 + 1, y * 2 + 1],
    ];

    let mut child_images: [Option<image::DynamicImage>; 4] = [None, None, None, None];

    let mut headers = HeaderMap::new();

    headers.append(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}.{}", worker_id, token))?,
    );

    for (i, [x_child, y_child]) in children_tiles.iter().enumerate() {
        let child_tile_url = format!(
            "{}/api/map-generation/pyramid-steps/{}/{}/{}/{}",
            base_api_url,
            area_id,
            z + 1,
            x_child,
            y_child
        );

        let child_tile_x_path = area_tiles_dir_path
            .join((z + 1).to_string())
            .join(&x_child.to_string());

        if !child_tile_x_path.exists() {
            create_dir_all(&child_tile_x_path)?;
        }

        let child_tile_path = child_tile_x_path.join(format!("{}.png", y_child));
        let _ = download_file(&child_tile_url, &child_tile_path, Some(headers.clone()));

        let child_image = image::open(child_tile_path).ok();
        child_images[i] = child_image;
    }

    let duration = start.elapsed();

    info!(
        "Zoom={} x={} y={}, children tiles (maybe) downloaded in {:.1?}",
        z, x, y, duration
    );

    info!(
        "Zoom={} x={} y={}, merging and resizing children tiles",
        z, x, y
    );

    let start = Instant::now();

    // Merging children tiles
    let tile_x_path = area_tiles_dir_path
        .join(&z.to_string())
        .join(&x.to_string());

    if !tile_x_path.exists() {
        create_dir_all(&tile_x_path)?;
    }

    let mut tile_image =
        RgbaImage::from_pixel(TILE_PIXEL_SIZE * 2, TILE_PIXEL_SIZE * 2, Rgba([0, 0, 0, 0]));

    if let Some(image) = &child_images[0] {
        tile_image.copy_from(&image.to_rgba8(), 0, 0)?;
    }

    if let Some(image) = &child_images[1] {
        tile_image.copy_from(&image.to_rgba8(), TILE_PIXEL_SIZE, 0)?;
    }

    if let Some(image) = &child_images[2] {
        tile_image.copy_from(&image.to_rgba8(), 0, TILE_PIXEL_SIZE)?;
    }

    if let Some(image) = &child_images[3] {
        tile_image.copy_from(&image.to_rgba8(), TILE_PIXEL_SIZE, TILE_PIXEL_SIZE)?;
    }

    // Saving on disk and resizing
    let tile_path = tile_x_path.join(format!("{}.png", y));
    tile_image.save(&tile_path)?;
    resize_image_in_place(&tile_path, TILE_PIXEL_SIZE, TILE_PIXEL_SIZE)?;

    let duration = start.elapsed();

    info!(
        "Zoom={} x={} y={}, children tiles merged and resized in {:.1?}",
        z, x, y, duration
    );

    // Uploading tile
    upload_tile(
        base_api_url,
        &tile_path,
        format!("{}.png", y),
        &area_id,
        z,
        x,
        y,
        worker_id,
        token,
    )?;

    Ok(())
}

/// Split an image in four parts: Top-left, Top-right, Bottom-left and Bottom-right
///
/// /// # Arguments
///
/// * `input_path` - The path of the image to be splitted in four.
/// * `output_paths` - An array of path where the resulting images should be writen.
///     [Top-left, Top-right, Bottom-left, Bottom-right]
///
fn split_image_in_four(
    input_path: &PathBuf,
    output_paths: &[&PathBuf; 4],
) -> Result<(), Box<dyn std::error::Error>> {
    // Load the input image
    let img = image::open(&Path::new(input_path))?;
    let (width, height) = img.dimensions();

    let half_width = width / 2;
    let half_height = height / 2;

    // Define regions and save each quarter
    let regions = [
        (0, 0, half_width, half_height),                    // Top-left
        (half_width, 0, half_width, half_height),           // Top-right
        (0, half_height, half_width, half_height),          // Bottom-left
        (half_width, half_height, half_width, half_height), // Bottom-right
    ];

    for (i, &(x, y, w, h)) in regions.iter().enumerate() {
        let sub_image = img.view(x, y, w, h).to_image(); // Extract sub-image
        sub_image
            .save(&output_paths[i])
            .expect("Failed to save output image");
    }

    Ok(())
}

fn resize_image_in_place(
    image_path: &PathBuf,
    width: u32,
    height: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let img = image::open(&Path::new(image_path))?;
    let resized_img = img.resize(width, height, FilterType::Lanczos3);
    resized_img.save(image_path)?;

    Ok(())
}

fn upload_tile(
    base_api_url: &str,
    file_path: &PathBuf,
    file_name: String,
    area_id: &str,
    zoom: i32,
    x: i32,
    y: i32,
    worker_id: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Uploading tile zoom={} x={} y={}", zoom, x, y);
    let start = Instant::now();

    let client = Client::new();
    let file = read(file_path)?;

    let part = multipart::Part::bytes(file)
        .file_name(file_name)
        .mime_str("image/png")?;

    let form = multipart::Form::new().part("file", part);

    let url = format!(
        "{}/api/map-generation/pyramid-steps/{}/{}/{}/{}.png",
        base_api_url, area_id, zoom, x, y
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
            "Tile zoom={} x={} y={} uploaded in {:.1?}",
            zoom, x, y, duration
        );
    } else {
        error!(
            "Failed to upload tile zoom={} x={} y={}: {} {}",
            zoom,
            x,
            y,
            response.status(),
            response.text()?
        );
    }

    Ok(())
}
