"""Read-only gym badge summary for authenticated trainer profiles (V2.20.5)."""
from fastapi import APIRouter, Depends, HTTPException
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from routes_gyms import asegurar_tablas_gyms, obtener_catalogo_gyms_usuario

router_profile_badges = APIRouter()

@router_profile_badges.get("/battle/gyms/medallas")
def obtener_medallas_perfil(usuario=Depends(get_current_user)):
    conn = get_connection()
    cur = None
    try:
        cur = get_cursor(conn)
        asegurar_tablas_gyms(cur)
        rows = obtener_catalogo_gyms_usuario(cur, int(usuario["id"]))
        medallas = [{
            "codigo": row["codigo"],
            "region_codigo": row["region_codigo"],
            "region_nombre": row["region_nombre"],
            "medalla_nombre": row["medalla_nombre"],
            "medalla_slug": row["medalla_slug"],
            "badge_asset_path": row.get("badge_asset_path"),
            "completado": bool(row["completado"]),
        } for row in rows]
        conn.commit()
        return {"ok": True, "total": len(medallas), "ganadas": sum(1 for m in medallas if m["completado"]), "medallas": medallas}
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo cargar las medallas")
    finally:
        if cur is not None: cur.close()
        release_connection(conn)
