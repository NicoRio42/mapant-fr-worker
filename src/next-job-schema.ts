import z from "zod";

const lidarJobSchema = z.object({
  type: z.literal("lidar"),
  data: z.object({
    tileId: z.string(),
    tileUrl: z.string(),
  }),
});

const renderJobSchema = z.object({
  type: z.literal("render"),
  data: z.object({
    tileId: z.string(),
    neigbhoringTilesIds: z.string().array(),
  }),
});

const pyramidJobSchema = z.object({
  type: z.literal("pyramid"),
  data: z.object({
    x: z.string(),
    y: z.string(),
    z: z.string(),
    baseZoomLevelTileId: z.string().nullable(),
    areaId: z.string(),
  }),
});

export const jobSchema = z.union([
  lidarJobSchema,
  renderJobSchema,
  pyramidJobSchema,
  z.object({
    type: z.literal("noJobLeft"),
  }),
]);

export type LidarJob = z.infer<typeof lidarJobSchema>;
export type RenderJob = z.infer<typeof renderJobSchema>;
export type PyramidJob = z.infer<typeof pyramidJobSchema>;
