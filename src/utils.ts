type LogLevel = "info" | "warn" | "error";

export function log(
  content: any,
  args?: { threadNumber?: number; level?: LogLevel },
) {
  const level = args?.level ?? "info";
  const threadNumber = args?.threadNumber?.toString() ?? "main";

  const fun = console[level === "error" ? "error" : level === "warn" ? "warn" : "log"];
  const color = level === "error" ? "red" : level === "warn" ? "orange" : undefined;

  fun(
    `%c[${level.toUpperCase()} ${new Date().toISOString()} Thread(${threadNumber})] ${content}`,
    color !== undefined ? `color: ${color}` : "",
  );
}

export async function isGdalIsAvailable(): Promise<boolean> {
  try {
    const command = new Deno.Command("gdalinfo", { args: ["--version"] });
    const { success } = await command.output();
    return success;
  } catch {
    return false;
  }
}

export async function isPdalIsAvailable(): Promise<boolean> {
  try {
    const command = new Deno.Command("pdal", { args: ["--version"] });
    const { success } = await command.output();
    return success;
  } catch {
    return false;
  }
}
