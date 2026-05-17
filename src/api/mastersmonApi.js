// src/api/mastersmonApi.js
// MastersMon frontend API layer.
// Usa VITE_API_URL si existe; si no, usa la API publicada en Render.

const DEFAULT_API_URL = "https://mastersmon-api.onrender.com";

export const API_BASE_URL =
  import.meta?.env?.VITE_API_URL?.replace(/\/$/, "") || DEFAULT_API_URL;

async function request(path, options = {}) {
  const url = `${API_BASE_URL}${path}`;

  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  const data = isJson ? await response.json() : await response.text();

  if (!response.ok) {
    const message =
      typeof data === "object" && data?.error
        ? data.error
        : `Request failed: ${response.status}`;

    throw new Error(message);
  }

  return data;
}

// =======================================================
// Health
// =======================================================

export function getHealth() {
  return request("/api/health");
}

// =======================================================
// Demo player
// =======================================================

export function getDemoProfile() {
  return request("/api/demo/me");
}

export function getDemoInventory() {
  return request("/api/demo/inventory");
}

export function getDemoTeam() {
  return request("/api/demo/team");
}

export function getDemoCollection({ limit = 100 } = {}) {
  return request(`/api/demo/collection?limit=${encodeURIComponent(limit)}`);
}

export function getDemoPokedexSummary() {
  return request("/api/demo/pokedex-summary");
}

export function getDemoPokedex({ generation, caught } = {}) {
  const params = new URLSearchParams();

  if (generation) params.set("generation", String(generation));
  if (caught !== undefined && caught !== null) params.set("caught", String(caught));

  const query = params.toString();
  return request(`/api/demo/pokedex${query ? `?${query}` : ""}`);
}

// =======================================================
// Maps / spawns
// =======================================================

export function getMaps() {
  return request("/api/maps");
}

export function getMapSpawns(mapSlug) {
  if (!mapSlug) throw new Error("mapSlug is required");
  return request(`/api/maps/${encodeURIComponent(mapSlug)}/spawns`);
}

// =======================================================
// Encounter / capture
// =======================================================

export function createDemoEncounter(mapSlug = "bosque-verde") {
  return request("/api/demo/encounters", {
    method: "POST",
    body: JSON.stringify({ mapSlug }),
  });
}

export function getActiveDemoEncounters() {
  return request("/api/demo/encounters/active");
}

export function captureDemoEncounter(encounterId, ballSlug = "poke-ball") {
  if (!encounterId) throw new Error("encounterId is required");

  return request("/api/demo/captures", {
    method: "POST",
    body: JSON.stringify({
      encounterId,
      ballSlug,
    }),
  });
}

export function captureLatestDemoEncounter(ballSlug = "poke-ball") {
  return request("/api/demo/captures/latest", {
    method: "POST",
    body: JSON.stringify({ ballSlug }),
  });
}

// =======================================================
// Server activity
// =======================================================

export function getServerRecentCaptures({ limit = 20 } = {}) {
  return request(`/api/server/recent-captures?limit=${encodeURIComponent(limit)}`);
}

// =======================================================
// Asset helpers
// =======================================================

export function getAssetUrl(path) {
  if (!path) return "";

  // Si ya es URL completa, la devolvemos igual.
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }

  // Tus assets viven en el frontend, no en el backend.
  // Por eso usamos la ruta relativa tal como viene de la DB: /img/...
  return path;
}
