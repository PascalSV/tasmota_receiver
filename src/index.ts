export interface Env {
    "TASMOTA-READINGS": D1Database;
    TASMOTA_INGEST_TOKEN: string;
}

interface RequestLogContext {
    cfRay: string | null;
    sourceIp: string | null;
    method: string;
    path: string;
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

function getRequestLogContext(request: Request, url: URL): RequestLogContext {
    return {
        cfRay: request.headers.get("cf-ray"),
        sourceIp: request.headers.get("CF-Connecting-IP"),
        method: request.method,
        path: url.pathname,
    };
}

export default {
    async fetch(request: Request, env: Env): Promise<Response> {
        const url = new URL(request.url);
        const requestContext = getRequestLogContext(request, url);

        if (url.pathname === "/health") {
            return Response.json({ ok: true });
        }

        if (request.method !== "POST" || url.pathname !== "/ingest") {
            console.warn("Unhandled request", requestContext);
            return new Response("Not Found", { status: 404 });
        }

        console.info("Ingest request received", {
            ...requestContext,
            contentType: request.headers.get("Content-Type"),
            contentLength: request.headers.get("Content-Length"),
            authorizationPresent: request.headers.has("Authorization"),
        });

        const token = getBearerToken(request.headers.get("Authorization"));
        if (!token || token !== env.TASMOTA_INGEST_TOKEN) {
            console.warn("Ingest request rejected", {
                ...requestContext,
                reason: !token ? "missing_or_invalid_authorization_header" : "token_mismatch",
            });
            return ignoreRequest();
        }

        let payload: unknown;
        try {
            payload = await request.json();
        } catch (error) {
            console.warn("Invalid JSON payload", {
                ...requestContext,
                error: error instanceof Error ? error.message : String(error),
            });
            return new Response("Invalid JSON payload", { status: 400 });
        }

        const payloadRecord = typeof payload === "object" && payload !== null ? (payload as Record<string, unknown>) : null;
        const input = toFiniteNumber(payloadRecord?.input);
        const output = toFiniteNumber(payloadRecord?.output);

        if (input === null || output === null) {
            console.warn("Invalid ingest values", {
                ...requestContext,
                payloadKeys: payloadRecord ? Object.keys(payloadRecord) : [],
                input,
                output,
            });
            return new Response("Missing or invalid input/output", { status: 400 });
        }

        const sourceIp = request.headers.get("CF-Connecting-IP");
        const userAgent = request.headers.get("User-Agent");
        const receivedAt = formatUtcTimestamp(new Date());

        console.info("Writing ingest to D1", {
            ...requestContext,
            input,
            output,
            receivedAt,
        });

        try {
            const result = await env["TASMOTA-READINGS"].prepare(
                `INSERT INTO tasmota_readings (input_kwh, output_kwh, source_ip, user_agent, received_at)
       VALUES (?1, ?2, ?3, ?4, ?5)`
            )
                .bind(input, output, sourceIp, userAgent, receivedAt)
                .run();

            console.info("Ingest stored successfully", {
                ...requestContext,
                rowId: result.meta.last_row_id,
                rowsWritten: result.meta.changes,
            });
        } catch (error) {
            console.error("Failed to store ingest", {
                ...requestContext,
                input,
                output,
                receivedAt,
                error: error instanceof Error ? error.message : String(error),
            });
            return new Response("Database write failed", { status: 500 });
        }

        return Response.json({ ok: true });
    },
};
