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
import { fetchWithRetryAndTimeout, log, removeIfExists } from "./utils.ts";
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
  const { mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId, threadNumber } = options;

  const zoom12Tiles = getChildrenTiles({ zoom: 11, x: xBase, y: yBase });

  const allTiles = [
    { zoom: 11, x: xBase, y: yBase },
    ...zoom12Tiles,
    ...zoom12Tiles.flatMap(getChildrenTiles),
  ];

  try {
    log(`Tile zoom=11 x=${xBase} y=${yBase} | Generating pyramid tiles for zoom 11 and higher`, {
      level: "info",
      threadNumber,
    });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Downloading base tile`, {
      level: "info",
      threadNumber,
    });

    const zoom11XPath = join(areaTilesDirPath, "11", xBase.toString());
    await ensureDir(zoom11XPath);
    const zoom11TilePath = join(zoom11XPath, `${yBase}.png`);
    const zoom11TileUrl = `${mapantApiBaseUrl}${RENDER_STEP_ENDPOINT_PATH}/${tileId}/full-map`;
    await downloadPng(zoom11TileUrl, zoom11TilePath, options);

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Base tile downloaded`, {
      level: "info",
      threadNumber,
    });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Generating zoom 12 tiles`, {
      level: "info",
      threadNumber,
    });

    await createChildTiles({ zoom: 11, x: xBase, y: yBase, areaTilesDirPath });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Zoom 12 tiles generated`, {
      level: "info",
      threadNumber,
    });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Generating zoom 13 tiles`, {
      level: "info",
      threadNumber,
    });

    await Promise.all(
      zoom12Tiles.map(({ x, y }) => createChildTiles({ zoom: 12, x, y, areaTilesDirPath })),
    );

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Zoom 13 tiles generated`, {
      level: "info",
      threadNumber,
    });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | resizing all tiles`, {
      level: "info",
      threadNumber,
    });

    await Promise.all(allTiles.map((tile) => resizeTile({ ...tile, areaTilesDirPath })));

    log(`Tile zoom=11 x=${xBase} y=${yBase} | All tiles resized`, {
      level: "info",
      threadNumber,
    });

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Uploading tiles`, {
      level: "info",
      threadNumber,
    });

    const formData = new FormData();

    await Promise.all(
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

    await fetchWithRetryAndTimeout(
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

    log(`Tile zoom=11 x=${xBase} y=${yBase} | Tiles uploaded`, {
      level: "info",
      threadNumber,
    });
  } catch (e) {
    await Promise.allSettled(
      allTiles.map(({ zoom, x, y }) =>
        removeIfExists(join(areaTilesDirPath, zoom.toString(), x.toString(), `${y}.png`))
      ),
    );

    throw e;
  }
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
  const xTilePath = join(areaTilesDirPath, zoom.toString(), x.toString());

  await Promise.all([
    ensureDir(xTilePath),
    ensureDir(join(areaTilesDirPath, (zoom + 1).toString(), (x * 2).toString())),
    ensureDir(join(areaTilesDirPath, (zoom + 1).toString(), (x * 2 + 1).toString())),
  ]);

  const tilePath = join(xTilePath, `${y}.png`);

  const childrenTiles = [
    { xChild: x * 2, yChild: y * 2 },
    { xChild: x * 2 + 1, yChild: y * 2 },
    { xChild: x * 2, yChild: y * 2 + 1 },
    { xChild: x * 2 + 1, yChild: y * 2 + 1 },
  ].map(({ xChild, yChild }) => ({
    url: `${baseUrl}/${xChild}/${yChild}`,
    path: join(areaTilesDirPath, (zoom + 1).toString(), xChild.toString(), `${yChild}.png`),
  }));

  try {
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
      overlays.push({
        input: bottomRightResult.value,
        top: TILE_PIXEL_SIZE,
        left: TILE_PIXEL_SIZE,
      });
    }

    const buffer = await sharp({
      create: {
        width: TILE_PIXEL_SIZE * 2,
        height: TILE_PIXEL_SIZE * 2,
        channels: 4,
        background: { r: 255, g: 255, b: 255, alpha: 0 },
      },
    }).composite(overlays).png().toBuffer();

    await sharp(buffer).resize({ width: TILE_PIXEL_SIZE, height: TILE_PIXEL_SIZE }).toFile(
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
  } catch (e) {
    await removeIfExists(tilePath);

    throw e;
  }
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

  const buffer = await sharp(path).resize({ width: TILE_PIXEL_SIZE, height: TILE_PIXEL_SIZE })
    .toBuffer();

  await sharp(buffer).toFile(path);
}

function getChildrenTiles({ zoom, x, y }: { zoom: number; x: number; y: number }) {
  return [
    { zoom: zoom + 1, x: x * 2, y: y * 2 },
    { zoom: zoom + 1, x: x * 2 + 1, y: y * 2 },
    { zoom: zoom + 1, x: x * 2, y: y * 2 + 1 },
    { zoom: zoom + 1, x: x * 2 + 1, y: y * 2 + 1 },
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

  const metadata = await sharp(parentTilePath).metadata();
  if (
    metadata.width === undefined || metadata.height === undefined ||
    metadata.width !== metadata.height
  ) {
    throw new Error("Ban image format");
  }

  const side = Math.floor(metadata.width / 2);

  const extractOptions = [
    { left: 0, top: 0, path: join(upperZoomXPath, `${y * 2}.png`) },
    { left: side, top: 0, path: join(upperZoomXPlus1Path, `${y * 2}.png`) },
    { left: 0, top: side, path: join(upperZoomXPath, `${y * 2 + 1}.png`) },
    { left: side, top: side, path: join(upperZoomXPlus1Path, `${y * 2 + 1}.png`) },
  ];

  await Promise.all(
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
