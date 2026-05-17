# MastersMon Frontend API Layer

Copia estos archivos en `mastersmon-frontend`.

## Archivos

```txt
src/api/mastersmonApi.js
src/hooks/useMastersmonDemo.js
src/components/MastersmonApiDemo.jsx
.env.example
```

## Variables

En el frontend crea o actualiza `.env`:

```txt
VITE_API_URL=https://mastersmon-api.onrender.com
```

Luego reinicia Vite.

## Uso básico

```js
import {
  getDemoProfile,
  getDemoInventory,
  getDemoCollection,
  createDemoEncounter,
  captureLatestDemoEncounter,
} from "./api/mastersmonApi";

const profile = await getDemoProfile();
const inventory = await getDemoInventory();
const encounter = await createDemoEncounter("bosque-verde");
const result = await captureLatestDemoEncounter("poke-ball");
const collection = await getDemoCollection();
```

## Prueba rápida en React

Puedes importar el componente temporal:

```jsx
import MastersmonApiDemo from "./components/MastersmonApiDemo";

export default function App() {
  return <MastersmonApiDemo />;
}
```

Cuando confirmes que todo funciona, reemplaza este componente por integración real en tu hub.
