export interface Env {
    "TASMOTA-READINGS": D1Database;
    TASMOTA_INGEST_TOKEN: string;
}

function ignoreRequest(): Response {
    return new Response(null, { status: 204 });
}

function toFiniteNumber(value: unknown): number | null {
    if (typeof value === "number") {
        return Number.isFinite(value) ? value : null;
    }

    if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    return null;
}

function getBearerToken(authorizationHeader: string | null): string | null {
    if (!authorizationHeader) {
        return null;
    }

    const match = authorizationHeader.match(/^Bearer\s+(.+)$/i);
    return match?.[1]?.trim() || null;
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

        if (request.method !== "POST" || url.pathname !== "/ingest") {
            return new Response("Not Found", { status: 404 });
        }

        const token = getBearerToken(request.headers.get("Authorization"));
        if (!token || token !== env.TASMOTA_INGEST_TOKEN) {
            return ignoreRequest();
        }

        let payload: unknown;
        try {
            payload = await request.json();
        } catch {
            return new Response("Invalid JSON payload", { status: 400 });
        }

        const payloadRecord = typeof payload === "object" && payload !== null ? (payload as Record<string, unknown>) : null;
        const input = toFiniteNumber(payloadRecord?.input);
        const output = toFiniteNumber(payloadRecord?.output);

        if (input === null || output === null) {
            return new Response("Missing or invalid input/output", { status: 400 });
        }

        const sourceIp = request.headers.get("CF-Connecting-IP");
        const userAgent = request.headers.get("User-Agent");
        const receivedAt = formatUtcTimestamp(new Date());

        await env["TASMOTA-READINGS"].prepare(
            `INSERT INTO tasmota_readings (input_kwh, output_kwh, source_ip, user_agent, received_at)
       VALUES (?1, ?2, ?3, ?4, ?5)`
        )
            .bind(input, output, sourceIp, userAgent, receivedAt)
            .run();

        return Response.json({ ok: true });
    },
};
