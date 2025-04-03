import { ensureDir } from "@std/fs/ensure-dir";
import { JobHandlingAdditionnalArguments } from "./models.ts";
import { PyramidJob } from "./next-job-schema.ts";
import {
  HIGH_QUALITY_TILE_PIXEL_SIZE,
  PYRAMID_STEP_DIR_NAME,
  PYRAMID_STEP_ENDPOINT_PATH,
  RENDER_STEP_ENDPOINT_PATH,
  TILE_PIXEL_SIZE,
} from "./constants.ts";
import { basename, join } from "@std/path";
import { exists } from "@std/fs/exists";
import { fetchWithRetryAndTimeout, log } from "./utils.ts";
import sharp, { type OverlayOptions } from "sharp";

class NotFoundFetchError extends Error {}

export async function handlePyramidJob(
  { areaId, x, y, baseZoomLevelTileId, z }: PyramidJob["data"],
  options: JobHandlingAdditionnalArguments,
) {
  await ensureDir(PYRAMID_STEP_DIR_NAME);
  const areaTilesDirPath = join(PYRAMID_STEP_DIR_NAME, areaId);
  await ensureDir(areaTilesDirPath);

  if (baseZoomLevelTileId !== null) {
    await pyramidStepBaseZoomLevel(
      { areaId, areaTilesDirPath, xBase: x, yBase: y, tileId: baseZoomLevelTileId },
      options,
    );
  } else {
    await pyramidStepLowerZoomLevel(
      { areaId, areaTilesDirPath, zoom: z, x, y },
      options,
    );
  }
}

async function pyramidStepBaseZoomLevel(
  { areaId, areaTilesDirPath, xBase, yBase, tileId }: {
    areaId: string;
    areaTilesDirPath: string;
    xBase: number;
    yBase: number;
    tileId: string;
  },
  options: JobHandlingAdditionnalArguments,
) {
  const { mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId } = options;

  // Zoom 11
  const zoom11XPath = join(areaTilesDirPath, "11", xBase.toString());
  await ensureDir(zoom11XPath);
  const zoom11TilePath = join(zoom11XPath, `${yBase}.png`);
  const zoom11TileUrl = `${mapantApiBaseUrl}${RENDER_STEP_ENDPOINT_PATH}/${tileId}/full-map`;
  await downloadPng(zoom11TileUrl, zoom11TilePath, options);

  // Creating child tiles
  await createChildTiles({ zoom: 11, x: xBase, y: yBase, areaTilesDirPath });
  const zoom12Tiles = getChildrenTiles({ zoom: 11, x: xBase, y: yBase });

  await Promise.allSettled(
    zoom12Tiles.map(({ x, y }) => createChildTiles({ zoom: 12, x, y, areaTilesDirPath })),
  );

  const allTiles = [
    { zoom: 11, x: xBase, y: yBase },
    ...zoom12Tiles,
    ...zoom12Tiles.flatMap(getChildrenTiles),
  ];

  await Promise.allSettled(allTiles.map((tile) => resizeTile({ ...tile, areaTilesDirPath })));

  const formData = new FormData();

  await Promise.allSettled(
    allTiles.map(async ({ zoom, x, y }) => {
      const file = await Deno.readFile(
        join(areaTilesDirPath, zoom.toString(), x.toString(), `${y}.png`),
      );

      formData.append(
        `${zoom}_${x}_${y}`,
        new Blob([file], { type: "image/png" }),
        `${y}.png`,
      );
    }),
  );

  await fetch(
    `${mapantApiBaseUrl}${PYRAMID_STEP_ENDPOINT_PATH}/${areaId}/base-level/${xBase}/${yBase}`,
    {
      method: "POST",
      body: formData,
      headers: {
        "Origin": mapantApiBaseUrl,
        "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
      },
    },
  );
}

async function pyramidStepLowerZoomLevel(
  { areaId, areaTilesDirPath, zoom, x, y }: {
    areaId: string;
    areaTilesDirPath: string;
    zoom: number;
    x: number;
    y: number;
  },
  options: JobHandlingAdditionnalArguments,
) {
  const { mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId, threadNumber } = options;

  log(`Tile zoom=${zoom} x=${x} y=${y} | Generating pyramid tile`, {
    level: "info",
    threadNumber,
  });

  log(`Tile zoom=${zoom} x=${x} y=${y} | Downloading children tiles`, {
    level: "info",
    threadNumber,
  });

  const baseUrl = `${mapantApiBaseUrl}${PYRAMID_STEP_ENDPOINT_PATH}/${areaId}/${zoom + 1}`;

  const childrenTiles = [
    { xChild: x * 2, yChild: y * 2 },
    { xChild: x * 2 + 1, yChild: y * 2 },
    { xChild: x * 2, yChild: y * 2 + 1 },
    { xChild: x * 2 + 1, yChild: y * 2 + 1 },
  ].map(({ xChild, yChild }) => ({
    url: `${baseUrl}/${xChild}/${yChild}`,
    path: join(areaTilesDirPath, (zoom + 1).toString(), xChild.toString(), `${yChild}.png`),
  }));

  const fetchChildrenTilesResults = await Promise
    .allSettled(
      childrenTiles.map(async ({ url, path }) => {
        await downloadPng(url, path, options);
        return path;
      }),
    );

  // Throw only if not 404 error
  for (const result of fetchChildrenTilesResults) {
    if (result.status === "rejected" && !(result.reason instanceof NotFoundFetchError)) {
      throw result.reason;
    }
  }

  log(`Tile zoom=${zoom} x=${x} y=${y} | Children tiles downloaded`, {
    level: "info",
    threadNumber,
  });

  log(`Tile zoom=${zoom} x=${x} y=${y} | Merging and resizing children tiles`, {
    level: "info",
    threadNumber,
  });

  const [topLeftResult, topRightResult, bottomLeftResult, bottomRightResult] =
    fetchChildrenTilesResults;

  const overlays: OverlayOptions[] = [];

  if (topLeftResult.status === "fulfilled") {
    overlays.push({ input: topLeftResult.value, top: 0, left: 0 });
  }
  if (topRightResult.status === "fulfilled") {
    overlays.push({ input: topRightResult.value, top: 0, left: TILE_PIXEL_SIZE });
  }
  if (bottomLeftResult.status === "fulfilled") {
    overlays.push({ input: bottomLeftResult.value, top: TILE_PIXEL_SIZE, left: 0 });
  }
  if (bottomRightResult.status === "fulfilled") {
    overlays.push({ input: bottomRightResult.value, top: TILE_PIXEL_SIZE, left: TILE_PIXEL_SIZE });
  }

  const tilePath = join(areaTilesDirPath, zoom.toString(), x.toString(), `${y}.png`);

  await sharp({
    create: {
      width: TILE_PIXEL_SIZE * 2,
      height: TILE_PIXEL_SIZE * 2,
      channels: 4,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    },
  }).composite(overlays).toFile(tilePath);

  await sharp(tilePath).resize({ width: TILE_PIXEL_SIZE, height: TILE_PIXEL_SIZE }).toFile(
    tilePath,
  );

  log(`Tile zoom=${zoom} x=${x} y=${y} | Children tiles merged and resized`, {
    level: "info",
    threadNumber,
  });

  log(`Tile zoom=${zoom} x=${x} y=${y} | Uploading tile`, {
    level: "info",
    threadNumber,
  });

  const formData = new FormData();

  formData.append(
    "file",
    new Blob([await Deno.readFile(tilePath)], { type: "image/png" }),
    `${y}.png`,
  );

  await fetchWithRetryAndTimeout(
    `${mapantApiBaseUrl}${PYRAMID_STEP_ENDPOINT_PATH}/${areaId}/${zoom}/${x}/${y}`,
    {
      method: "POST",
      body: formData,
      headers: {
        "Origin": mapantApiBaseUrl,
        "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
      },
    },
  );

  log(`Tile zoom=${zoom} x=${x} y=${y} | Tile uploaded`, {
    level: "info",
    threadNumber,
  });
}

async function resizeTile(
  { zoom, x, y, areaTilesDirPath }: {
    zoom: number;
    x: number;
    y: number;
    areaTilesDirPath: string;
  },
) {
  const path = join(areaTilesDirPath, zoom.toString(), x.toString(), `${y}.png`);
  await sharp(path).resize({ width: TILE_PIXEL_SIZE, height: TILE_PIXEL_SIZE }).toFile(path);
}

function getChildrenTiles({ zoom, x, y }: { zoom: number; x: number; y: number }) {
  return [
    { zoom: zoom * 2, x: x * 2, y: y * 2 },
    { zoom: zoom * 2, x: x * 2 + 1, y: y * 2 },
    { zoom: zoom * 2, x: x * 2, y: y * 2 + 1 },
    { zoom: zoom * 2, x: x * 2, y: y * 2 + 1 },
  ];
}

async function createChildTiles(
  { zoom, x, y, areaTilesDirPath }: {
    zoom: number;
    x: number;
    y: number;
    areaTilesDirPath: string;
  },
) {
  const parentTilePath = join(areaTilesDirPath, zoom.toString(), x.toString(), `${y}.png`);
  const upperZoomPath = join(areaTilesDirPath, (zoom + 1).toString());
  const upperZoomXPath = join(upperZoomPath, (x * 2).toString());
  await ensureDir(upperZoomXPath);
  const upperZoomXPlus1Path = join(upperZoomPath, (x * 2 + 1).toString());
  await ensureDir(upperZoomXPlus1Path);

  const side = HIGH_QUALITY_TILE_PIXEL_SIZE / 2;

  const extractOptions = [
    { left: 0, top: 0, path: join(upperZoomXPath, `${y * 2}.png`) },
    { left: side, top: 0, path: join(upperZoomXPlus1Path, `${y * 2}.png`) },
    { left: 0, top: side, path: join(upperZoomXPath, `${y * 2 + 1}.png`) },
    { left: side, top: side, path: join(upperZoomXPlus1Path, `${y * 2 + 1}.png`) },
  ];

  await Promise.allSettled(
    extractOptions.map(({ left, top, path }) =>
      sharp(parentTilePath).extract({ left, top, width: side, height: side }).toFile(path)
    ),
  );
}

const downloadJobs: Map<string, Promise<void>> = new Map();

async function downloadPng(
  url: string,
  path: string,
  options: JobHandlingAdditionnalArguments,
): Promise<void> {
  const { threadNumber, mapantApiToken, mapantApiWorkerId } = options;
  const filename = basename(path);

  if (await exists(path)) {
    log(`File ${filename} already on disk`, { level: "info", threadNumber });
    return;
  }

  const onGoingJob = downloadJobs.get(url);

  if (onGoingJob !== undefined) {
    log(`File ${filename} already being downloaded`, { level: "info", threadNumber });
    return onGoingJob;
  }

  log(`Downloading file ${filename}`, { level: "info", threadNumber });

  const job = fetch(url, {
    headers: {
      "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
    },
  })
    .then(async (response) => {
      if (!response.ok || response.body === null) {
        const errorMessage =
          `Could not fetch file ${filename}, Status: ${response.status}, ${await response.text()}`;

        if (response.status === 404) throw new NotFoundFetchError();

        throw new Error(
          `Could not fetch file ${filename}, Status: ${response.status}, ${await response.text()}`,
        );
      }

      const file = await Deno.open(path, { write: true, create: true });
      return response.body.pipeTo(file.writable);
    })
    .then(() => {
      downloadJobs.delete(url);
    });

  downloadJobs.set(url, job);

  return job;
}
