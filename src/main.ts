import { parseArgs } from "@std/cli/parse-args";
import {
  MAPANT_API_BASE_URL,
  NEXT_JOB_ENDPOINT_PATH,
  RETRY_TIMEOUT_AFTER_NO_JOB_LEFT,
} from "./constants.ts";
import { jobSchema } from "./next-job-schema.ts";
import { handleLidarJob } from "./lidar.ts";
import { handleRenderJob } from "./render.ts";
import { handlePyramidJob } from "./pyramid.ts";
import { isCassiniAvailable, isGdalAvailable, isPdalAvailable, log } from "./utils.ts";

main();

async function main() {
  if (!(await isGdalAvailable())) {
    log("GDAL is not available on your machine. Please install GDAL", { level: "error" });
    return;
  }

  if (!(await isPdalAvailable())) {
    log("PDAL is not available on your machine. Please install PDAL", { level: "error" });
    return;
  }

  if (!(await isCassiniAvailable())) {
    log("Cassini is not available on your machine. Please install Cassini", { level: "error" });
    return;
  }

  const args = parseArgs(Deno.args);

  const threads = typeof args.threads === "number" && !isNaN(args.threads) ? args.threads : 1;

  const mapantApiWorkerId = Deno.env.get("MAPANT_API_WORKER_ID");
  const mapantApiToken = Deno.env.get("MAPANT_API_TOKEN");

  const mapantApiBaseUrl = Deno.env.get("MAPANT_API_BASE_URL") ?? MAPANT_API_BASE_URL;

  if (mapantApiWorkerId === undefined) {
    log("MAPANT_API_WORKER_ID environment variable not set.", { level: "error" });
    return;
  }

  if (mapantApiToken === undefined) {
    log("MAPANT_API_TOKEN environment variable not set.", { level: "error" });
    return;
  }

  const nextJobUrl = `${mapantApiBaseUrl}${NEXT_JOB_ENDPOINT_PATH}`;

  for (const threadIndex of Array(threads).keys()) {
    let threadNumber = threadIndex + 1;

    while (true) {
      try {
        await getAndHandleNextJob({
          threadNumber,
          nextJobUrl,
          mapantApiWorkerId,
          mapantApiToken,
          mapantApiBaseUrl,
        });
      } catch (e) {
        log(e, { level: "error", threadNumber });
      }
    }
  }
}

async function getAndHandleNextJob({
  threadNumber,
  nextJobUrl,
  mapantApiWorkerId,
  mapantApiToken,
  mapantApiBaseUrl,
}: {
  threadNumber: number;
  mapantApiWorkerId: string;
  mapantApiToken: string;
  nextJobUrl: string;
  mapantApiBaseUrl: string;
}) {
  const response = await fetch(nextJobUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
    },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to call next job endpoint.\nResponse status: ${response.status}.\n${await response
        .text()}`,
    );
  }

  const nextJob = jobSchema.parse(await response.json());

  if (nextJob.type === "lidar") {
    log(`Handling lidar job for tile with id ${nextJob.data.tileId}`, {
      threadNumber,
      level: "info",
    });

    const t0 = performance.now();

    await handleLidarJob(nextJob.data, {
      threadNumber,
      mapantApiWorkerId,
      mapantApiToken,
      mapantApiBaseUrl,
    });

    const t1 = performance.now();

    log(
      `Lidar job for tile with id ${nextJob.data.tileId} done in ${
        Math.round((t1 - t0) / 100) / 10
      } seconds`,
      { threadNumber, level: "info" },
    );
  }

  if (nextJob.type === "render") {
    log(`Handling render job for tile with id ${nextJob.data.tileId}`, {
      threadNumber,
      level: "info",
    });

    const t0 = performance.now();
    await handleRenderJob(nextJob.data);
    const t1 = performance.now();

    log(
      `Render job for tile with id ${nextJob.data.tileId} done in ${
        Math.round((t1 - t0) / 100) / 10
      } seconds`,
      { threadNumber, level: "info" },
    );
  }

  if (nextJob.type === "pyramid") {
    log(
      `Handling pyramid job for tile with zoom ${nextJob.data.z} x ${nextJob.data.x} y ${nextJob.data.y}`,
      { threadNumber, level: "info" },
    );

    const t0 = performance.now();
    await handlePyramidJob(nextJob.data);
    const t1 = performance.now();

    log(
      `Pyramid job for tile with zoom ${nextJob.data.z} x ${nextJob.data.x} y ${nextJob.data.y} done in ${
        Math.round((t1 - t0) / 100) / 10
      } seconds`,
      { threadNumber, level: "info" },
    );
  }

  if (nextJob.type === "noJobLeft") {
    log(`No job left, retrying in ${RETRY_TIMEOUT_AFTER_NO_JOB_LEFT / 1000} seconds`, {
      threadNumber,
      level: "info",
    });

    await new Promise((r) => setTimeout(r, RETRY_TIMEOUT_AFTER_NO_JOB_LEFT));
  }
}
