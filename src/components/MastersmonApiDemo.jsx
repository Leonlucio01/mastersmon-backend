// src/components/MastersmonApiDemo.jsx
// Componente temporal para probar la API desde el frontend.
// Si no tienes carpeta src/components, créala.

import { useMastersmonDemo } from "../hooks/useMastersmonDemo";
import { getAssetUrl } from "../api/mastersmonApi";

export default function MastersmonApiDemo() {
  const {
    loading,
    error,
    profile,
    inventory,
    team,
    collection,
    pokedexSummary,
    maps,
    currentEncounter,
    lastCaptureResult,
    findEncounter,
    captureLatest,
    refreshAll,
  } = useMastersmonDemo();

  return (
    <div style={{ padding: 24, color: "white", background: "#111827", minHeight: "100vh" }}>
      <h1>MastersMon API Demo</h1>

      {loading && <p>Cargando...</p>}
      {error && <p style={{ color: "#f87171" }}>{error}</p>}

      <button onClick={refreshAll}>Refrescar</button>
      <button onClick={() => findEncounter("bosque-verde")} style={{ marginLeft: 8 }}>
        Buscar en Bosque Verde
      </button>
      <button onClick={() => captureLatest("poke-ball")} style={{ marginLeft: 8 }}>
        Capturar último con Poké Ball
      </button>

      <section>
        <h2>Perfil</h2>
        <pre>{JSON.stringify(profile, null, 2)}</pre>
      </section>

      <section>
        <h2>Pokédex</h2>
        <pre>{JSON.stringify(pokedexSummary, null, 2)}</pre>
      </section>

      <section>
        <h2>Encuentro actual</h2>
        {currentEncounter ? (
          <div>
            <p>
              {currentEncounter.pokemon_name} Nv. {currentEncounter.level}
              {currentEncounter.is_shiny ? " ✨" : ""}
            </p>
            <img
              src={getAssetUrl(currentEncounter.selected_sprite_path)}
              alt={currentEncounter.pokemon_name}
              width={96}
              height={96}
            />
            <pre>{JSON.stringify(currentEncounter, null, 2)}</pre>
          </div>
        ) : (
          <p>No hay encuentro actual.</p>
        )}
      </section>

      <section>
        <h2>Resultado captura</h2>
        <pre>{JSON.stringify(lastCaptureResult, null, 2)}</pre>
      </section>

      <section>
        <h2>Equipo</h2>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          {team.map((slot) => (
            <div key={slot.slot_number} style={{ border: "1px solid #374151", padding: 12, width: 150 }}>
              <strong>Slot {slot.slot_number}</strong>
              {slot.player_monster_id ? (
                <>
                  <p>{slot.pokemon_name} Nv. {slot.level}</p>
                  <img
                    src={getAssetUrl(slot.selected_sprite_path)}
                    alt={slot.pokemon_name}
                    width={72}
                    height={72}
                  />
                </>
              ) : (
                <p>Vacío</p>
              )}
            </div>
          ))}
        </div>
      </section>

      <section>
        <h2>Inventario</h2>
        <ul>
          {inventory.map((item) => (
            <li key={item.item_id}>
              {item.display_name || item.item_name}: {item.quantity}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>Mapas</h2>
        <ul>
          {maps.map((map) => (
            <li key={map.map_id}>
              {map.name} ({map.slug}) — spawns: {map.total_spawns}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>Mis capturas</h2>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          {collection.map((monster) => (
            <div key={monster.player_monster_id} style={{ border: "1px solid #374151", padding: 12, width: 160 }}>
              <strong>{monster.pokemon_name}</strong>
              <p>Nv. {monster.level} {monster.is_shiny ? "✨" : ""}</p>
              <img
                src={getAssetUrl(monster.selected_sprite_path)}
                alt={monster.pokemon_name}
                width={80}
                height={80}
              />
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
