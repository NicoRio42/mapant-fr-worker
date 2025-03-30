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

main();

function main() {
  const args = parseArgs(Deno.args);

  const threads =
    typeof args.threads === "number" && !isNaN(args.threads) ? args.threads : 1;

  const mapantApiWorkerId = Deno.env.get("MAPANT_API_WORKER_ID");
  const mapantApiToken = Deno.env.get("MAPANT_API_TOKEN");

  const mapantApiBaseUrl =
    Deno.env.get("MAPANT_API_BASE_URL") ?? MAPANT_API_BASE_URL;

  if (mapantApiWorkerId === undefined) {
    console.error("MAPANT_API_WORKER_ID environment variable not set.");
    return;
  }

  if (mapantApiToken === undefined) {
    console.error("MAPANT_API_TOKEN environment variable not set.");
    return;
  }

  const nextJobUrl = `${mapantApiBaseUrl}${NEXT_JOB_ENDPOINT_PATH}`;

  for (const threadIndex of Array(threads).keys()) {
    while (true) {
      try {
        getAndHandleNextJob({
          threadIndex,
          nextJobUrl,
          mapantApiWorkerId,
          mapantApiToken,
        });
      } catch (e) {
        console.error(`An error occured in thread ${threadIndex}.`, e);
      }
    }
  }
}

async function getAndHandleNextJob({
  threadIndex,
  nextJobUrl,
  mapantApiWorkerId,
  mapantApiToken,
}: {
  threadIndex: number;
  mapantApiWorkerId: string;
  mapantApiToken: string;
  nextJobUrl: string;
}) {
  const response = await fetch(nextJobUrl, {
    method: "POST",
    headers: { Authorization: `Bearer ${mapantApiWorkerId}.${mapantApiToken}` },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to call next job endpoint.\nResponse status: ${
        response.status
      }.\n${await response.text()}`
    );
  }

  const nextJob = jobSchema.parse(await response.json());

  if (nextJob.type === "lidar") await handleLidarJob(nextJob.data);
  if (nextJob.type === "render") await handleRenderJob(nextJob.data);
  if (nextJob.type === "pyramid") await handlePyramidJob(nextJob.data);
  if (nextJob.type === "noJobLeft")
    await new Promise((r) => setTimeout(r, RETRY_TIMEOUT_AFTER_NO_JOB_LEFT));
}
