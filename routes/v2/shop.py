from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_shop = APIRouter(prefix="/v2/shop", tags=["v2-shop"])

UTILITY_ITEMS = {
    "poke_ball": {"price_gold": 200, "pack_quantity": 5, "sort_order": 1},
    "super_ball": {"price_gold": 600, "pack_quantity": 3, "sort_order": 2},
    "ultra_ball": {"price_gold": 1200, "pack_quantity": 2, "sort_order": 3},
    "master_ball": {"price_gold": 50000, "pack_quantity": 1, "sort_order": 4},
    "potion": {"price_gold": 120, "pack_quantity": 2, "sort_order": 5},
    "super_potion": {"price_gold": 350, "pack_quantity": 2, "sort_order": 6},
}


class UtilityPurchasePayload(BaseModel):
    item_code: str = Field(min_length=1, max_length=80)
    quantity: int = Field(default=1, ge=1, le=25)


def _fetch_wallet_summary(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            c.code AS currency_code,
            c.name AS currency_name,
            c.icon_path,
            uw.balance
        FROM user_wallets uw
        JOIN currencies c ON c.id = uw.currency_id
        WHERE uw.user_id = %s
          AND c.is_active = TRUE
        ORDER BY c.id ASC
        """,
        (user_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _fetch_recent_wallet_activity(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            wt.id,
            wt.direction,
            wt.source_type,
            wt.source_ref,
            wt.amount,
            wt.balance_before,
            wt.balance_after,
            wt.notes,
            wt.created_at,
            c.code AS currency_code,
            c.name AS currency_name
        FROM wallet_transactions wt
        JOIN user_wallets uw ON uw.id = wt.wallet_id
        JOIN currencies c ON c.id = uw.currency_id
        WHERE uw.user_id = %s
        ORDER BY wt.created_at DESC, wt.id DESC
        LIMIT 8
        """,
        (user_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _fetch_inventory_summary(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            ic.code,
            ic.name,
            ic.item_kind,
            ic.icon_path,
            COALESCE(ui.quantity, 0) AS quantity
        FROM item_catalog ic
        LEFT JOIN user_inventory ui
          ON ui.item_id = ic.id
         AND ui.user_id = %s
        WHERE ic.is_active = TRUE
          AND ic.code = ANY(%s)
        ORDER BY ic.name ASC
        """,
        (user_id, list(UTILITY_ITEMS.keys())),
    )
    inventory = {}
    for row in cursor.fetchall():
        inventory[row["code"]] = dict(row)
    return inventory


def _fetch_active_products(cursor):
    cursor.execute(
        """
        SELECT COUNT(*) AS active_products
        FROM premium_products
        WHERE is_active = TRUE
        """
    )
    return int((cursor.fetchone() or {}).get("active_products") or 0)


def _fetch_utility_catalog(cursor, user_id: int):
    inventory = _fetch_inventory_summary(cursor, user_id)
    cursor.execute(
        """
        SELECT
            id,
            code,
            name,
            item_kind,
            description,
            icon_path,
            stack_limit,
            is_tradable
        FROM item_catalog
        WHERE is_active = TRUE
          AND code = ANY(%s)
        ORDER BY id ASC
        """,
        (list(UTILITY_ITEMS.keys()),),
    )
    items = []
    for row in cursor.fetchall():
        item = dict(row)
        config = UTILITY_ITEMS.get(item["code"], {})
        owned = inventory.get(item["code"], {})
        item["price_gold"] = config.get("price_gold", 0)
        item["pack_quantity"] = config.get("pack_quantity", 1)
        item["sort_order"] = config.get("sort_order", 999)
        item["owned_quantity"] = int(owned.get("quantity") or 0)
        items.append(item)
    items.sort(key=lambda value: (value.get("sort_order", 999), value.get("id", 0)))
    return items


@router_v2_shop.get("/catalog")
def get_shop_catalog():
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                id,
                code,
                product_type,
                name,
                description,
                soft_currency_amount,
                premium_currency_amount,
                benefit_code,
                price_usd,
                metadata
            FROM premium_products
            WHERE is_active = TRUE
            ORDER BY id ASC
            """
        )
        items = [dict(row) for row in cursor.fetchall()]

    return ok({"items": items})


@router_v2_shop.get("/utility-catalog")
def get_utility_catalog(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        wallets = _fetch_wallet_summary(cursor, current_user["id"])
        items = _fetch_utility_catalog(cursor, current_user["id"])
    return ok({"wallets": wallets, "items": items})


@router_v2_shop.get("/summary")
def get_shop_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        wallets = _fetch_wallet_summary(cursor, current_user["id"])
        active_products = _fetch_active_products(cursor)
        recent_wallet_activity = _fetch_recent_wallet_activity(cursor, current_user["id"])
        utility_inventory = list(_fetch_inventory_summary(cursor, current_user["id"]).values())

    return ok(
        {
            "wallets": wallets,
            "active_products": active_products,
            "recent_wallet_activity": recent_wallet_activity,
            "utility_inventory": utility_inventory,
        }
    )


@router_v2_shop.post("/utility-purchase")
def purchase_utility_item(payload: UtilityPurchasePayload, current_user: dict = Depends(get_current_user)):
    item_config = UTILITY_ITEMS.get(payload.item_code)
    if not item_config:
        fail("shop.item_not_supported", "Ese item todavia no esta disponible en el PokeMart.", 404)

    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT
                ic.id,
                ic.code,
                ic.name,
                ic.description,
                ic.icon_path
            FROM item_catalog ic
            WHERE ic.is_active = TRUE
              AND ic.code = %s
            LIMIT 1
            """,
            (payload.item_code,),
        )
        item = cursor.fetchone()
        if not item:
            fail("shop.item_missing", "No se encontro el item solicitado.", 404)

        cursor.execute(
            """
            SELECT
                uw.id,
                uw.balance
            FROM user_wallets uw
            JOIN currencies c ON c.id = uw.currency_id
            WHERE uw.user_id = %s
              AND c.code = 'gold'
            LIMIT 1
            FOR UPDATE
            """,
            (current_user["id"],),
        )
        wallet = cursor.fetchone()
        if not wallet:
            fail("shop.gold_wallet_missing", "No se encontro la wallet de gold para este usuario.", 404)

        total_cost = int(item_config["price_gold"]) * int(payload.quantity)
        if int(wallet["balance"] or 0) < total_cost:
            fail("shop.insufficient_funds", "No tienes suficiente gold para completar la compra.", 400)

        new_balance = int(wallet["balance"] or 0) - total_cost
        cursor.execute(
            """
            UPDATE user_wallets
            SET balance = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (new_balance, wallet["id"]),
        )

        cursor.execute(
            """
            INSERT INTO wallet_transactions (
                wallet_id,
                direction,
                source_type,
                source_ref,
                amount,
                balance_before,
                balance_after,
                notes,
                metadata
            )
            VALUES (
                %s,
                'debit',
                'shop',
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb
            )
            """,
            (
                wallet["id"],
                f"utility:{payload.item_code}",
                total_cost,
                wallet["balance"],
                new_balance,
                f"Compra de {item['name']} x{payload.quantity}",
                (
                    f'{{"item_code":"{payload.item_code}",'
                    f'"quantity":{payload.quantity},'
                    f'"pack_quantity":{item_config["pack_quantity"]}}}'
                ),
            ),
        )

        granted_quantity = int(payload.quantity) * int(item_config["pack_quantity"])
        cursor.execute(
            """
            INSERT INTO user_inventory (user_id, item_id, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, item_id)
            DO UPDATE SET
                quantity = user_inventory.quantity + EXCLUDED.quantity,
                updated_at = CURRENT_TIMESTAMP
            RETURNING quantity
            """,
            (current_user["id"], item["id"], granted_quantity),
        )
        inventory_row = cursor.fetchone()

        wallets = _fetch_wallet_summary(cursor, current_user["id"])
        recent_wallet_activity = _fetch_recent_wallet_activity(cursor, current_user["id"])

    return ok(
        {
            "item_code": payload.item_code,
            "item_name": item["name"],
            "quantity_purchased": int(payload.quantity),
            "granted_quantity": granted_quantity,
            "spent_gold": total_cost,
            "wallet_balance": new_balance,
            "inventory_quantity": int((inventory_row or {}).get("quantity") or 0),
            "wallets": wallets,
            "recent_wallet_activity": recent_wallet_activity,
        }
    )
