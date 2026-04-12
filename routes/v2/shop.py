from fastapi import APIRouter, Depends

from auth import get_current_user
from core.http import ok
from database import db_cursor

router_v2_shop = APIRouter(prefix="/v2/shop", tags=["v2-shop"])


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


@router_v2_shop.get("/summary")
def get_shop_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
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
            (current_user["id"],),
        )
        wallets = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT COUNT(*) AS active_products
            FROM premium_products
            WHERE is_active = TRUE
            """
        )
        active_products = int((cursor.fetchone() or {}).get("active_products") or 0)

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
            (current_user["id"],),
        )
        recent_wallet_activity = [dict(row) for row in cursor.fetchall()]

    return ok(
        {
            "wallets": wallets,
            "active_products": active_products,
            "recent_wallet_activity": recent_wallet_activity,
        }
    )
