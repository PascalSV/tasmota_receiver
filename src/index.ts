export interface Env {
  DB: D1Database;
  INGEST_TOKEN: string;
}

function ignoreRequest(): Response {
  return new Response(null, { status: 204 });
}

function toFiniteNumber(value: string | null): number | null {
  if (value === null) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatUtcTimestamp(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} UTC`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({ ok: true });
    }

    if (request.method !== "GET" || url.pathname !== "/data") {
      return new Response("Not Found", { status: 404 });
    }

    const token = url.searchParams.get("token");
    if (!token || token !== env.INGEST_TOKEN) {
      return ignoreRequest();
    }

    const input = toFiniteNumber(url.searchParams.get("input"));
    const output = toFiniteNumber(url.searchParams.get("output"));

    if (input === null || output === null) {
      return new Response("Missing or invalid input/output", { status: 400 });
    }

    const sourceIp = request.headers.get("CF-Connecting-IP");
    const userAgent = request.headers.get("User-Agent");
    const receivedAt = formatUtcTimestamp(new Date());

    await env.DB.prepare(
      `INSERT INTO meter_readings (input_kwh, output_kwh, source_ip, user_agent, received_at)
       VALUES (?1, ?2, ?3, ?4, ?5)`
    )
      .bind(input, output, sourceIp, userAgent, receivedAt)
      .run();

    return Response.json({ ok: true });
  },
};
