import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_trade = APIRouter(prefix="/v2/trade", tags=["v2-trade"])


class TradeCurrencyLinePayload(BaseModel):
    currency_code: str
    amount: int = Field(..., gt=0, le=999999999)


class CreateTradeOfferPayload(BaseModel):
    notes: str | None = Field(default=None, max_length=255)
    expires_in_hours: int = Field(default=72, ge=1, le=168)
    offered_user_pokemon_ids: list[int] = Field(default_factory=list, max_length=6)
    requested_currencies: list[TradeCurrencyLinePayload] = Field(default_factory=list, max_length=4)


def _offer_pokemon_rows(cursor, trade_offer_id: int) -> list[dict]:
    cursor.execute(
        """
        SELECT
            topk.user_pokemon_id,
            topk.offered_by_user_id,
            COALESCE(NULLIF(up.nickname, ''), ps.name) AS display_name,
            up.species_id,
            up.level,
            up.is_shiny,
            up.is_favorite,
            COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
            COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
            COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
        FROM trade_offer_pokemon topk
        JOIN user_pokemon up ON up.id = topk.user_pokemon_id
        JOIN pokemon_species ps ON ps.id = up.species_id
        LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
        LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
        WHERE topk.trade_offer_id = %s
        ORDER BY topk.id ASC
        """,
        (trade_offer_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _offer_currency_rows(cursor, trade_offer_id: int) -> tuple[list[dict], list[dict]]:
    cursor.execute(
        """
        SELECT
            toc.amount,
            toc.is_requested,
            c.code AS currency_code,
            c.name AS currency_name,
            c.icon_path AS currency_icon
        FROM trade_offer_currencies toc
        JOIN currencies c ON c.id = toc.currency_id
        WHERE toc.trade_offer_id = %s
        ORDER BY toc.is_requested DESC, toc.id ASC
        """,
        (trade_offer_id,),
    )
    offered = []
    requested = []
    for row in cursor.fetchall():
        payload = dict(row)
        if payload["is_requested"]:
            requested.append(payload)
        else:
            offered.append(payload)
    return offered, requested


def _offer_detail(cursor, trade_offer_id: int, current_user_id: int | None = None) -> dict | None:
    cursor.execute(
        """
        SELECT
            o.id,
            o.created_by_user_id,
            o.status,
            o.notes,
            o.expires_at,
            o.created_at,
            o.updated_at,
            up.display_name AS created_by_name,
            up.avatar_code AS created_by_avatar,
            up.trainer_team AS created_by_team
        FROM trade_offers o
        JOIN user_profiles up ON up.user_id = o.created_by_user_id
        WHERE o.id = %s
        LIMIT 1
        """,
        (trade_offer_id,),
    )
    offer = cursor.fetchone()
    if not offer:
        return None

    offer_payload = dict(offer)
    offered_pokemon = _offer_pokemon_rows(cursor, trade_offer_id)
    offered_currencies, requested_currencies = _offer_currency_rows(cursor, trade_offer_id)

    offer_payload["offered_pokemon"] = offered_pokemon
    offer_payload["offered_currencies"] = offered_currencies
    offer_payload["requested_currencies"] = requested_currencies
    offer_payload["is_owner"] = current_user_id == offer_payload["created_by_user_id"]
    offer_payload["can_cancel"] = offer_payload["is_owner"] and offer_payload["status"] in {"draft", "open"}
    offer_payload["is_expired"] = bool(
        offer_payload["expires_at"] and offer_payload["expires_at"] <= datetime.utcnow()
    )
    return offer_payload


def _release_trade_offer_locks(cursor, trade_offer_id: int):
    cursor.execute(
        """
        UPDATE user_pokemon up
        SET is_trade_locked = FALSE,
            is_market_locked = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE up.id IN (
            SELECT topk.user_pokemon_id
            FROM trade_offer_pokemon topk
            WHERE topk.trade_offer_id = %s
        )
        """,
        (trade_offer_id,),
    )


def _get_wallet_row(cursor, user_id: int, currency_code: str) -> dict | None:
    cursor.execute(
        """
        SELECT
            uw.id,
            uw.user_id,
            uw.currency_id,
            uw.balance,
            c.code AS currency_code,
            c.name AS currency_name
        FROM user_wallets uw
        JOIN currencies c ON c.id = uw.currency_id
        WHERE uw.user_id = %s
          AND c.code = %s
        LIMIT 1
        """,
        (user_id, currency_code),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def _record_wallet_transaction(
    cursor,
    *,
    wallet_id: int,
    direction: str,
    amount: int,
    balance_before: int,
    balance_after: int,
    source_ref: str,
    notes: str,
    metadata: dict,
):
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
        VALUES (%s, %s, 'trade', %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            wallet_id,
            direction,
            source_ref,
            amount,
            balance_before,
            balance_after,
            notes[:255],
            json.dumps(metadata),
        ),
    )


@router_v2_trade.get("/summary")
def get_trade_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'open') AS my_open_offers,
                COUNT(*) FILTER (WHERE status = 'accepted') AS my_accepted_offers,
                COUNT(*) FILTER (WHERE status = 'cancelled') AS my_cancelled_offers
            FROM trade_offers
            WHERE created_by_user_id = %s
            """,
            (current_user["id"],),
        )
        my_summary = dict(cursor.fetchone())

        cursor.execute(
            """
            SELECT COUNT(*) AS market_open_offers
            FROM trade_offers
            WHERE status = 'open'
              AND created_by_user_id <> %s
              AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            """,
            (current_user["id"],),
        )
        market_open_offers = cursor.fetchone()["market_open_offers"] or 0

        cursor.execute(
            """
            SELECT COUNT(*) AS available_pokemon
            FROM user_pokemon up
            WHERE up.user_id = %s
              AND up.is_locked = FALSE
              AND up.is_trade_locked = FALSE
              AND up.is_team_locked = FALSE
              AND up.is_market_locked = FALSE
              AND NOT EXISTS (
                    SELECT 1
                    FROM house_storage hs
                    WHERE hs.user_pokemon_id = up.id
              )
            """,
            (current_user["id"],),
        )
        available_pokemon = cursor.fetchone()["available_pokemon"] or 0

    return ok(
        {
            "my_open_offers": my_summary["my_open_offers"] or 0,
            "my_accepted_offers": my_summary["my_accepted_offers"] or 0,
            "my_cancelled_offers": my_summary["my_cancelled_offers"] or 0,
            "market_open_offers": market_open_offers,
            "available_pokemon": available_pokemon,
        }
    )


@router_v2_trade.get("/available-pokemon")
def get_trade_available_pokemon(
    limit: int = Query(default=60, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.id AS user_pokemon_id,
                up.species_id,
                COALESCE(NULLIF(up.nickname, ''), ps.name) AS display_name,
                up.level,
                up.is_shiny,
                up.is_favorite,
                up.captured_at,
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
            FROM user_pokemon up
            JOIN pokemon_species ps ON ps.id = up.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            WHERE up.user_id = %s
              AND up.is_locked = FALSE
              AND up.is_trade_locked = FALSE
              AND up.is_team_locked = FALSE
              AND up.is_market_locked = FALSE
              AND NOT EXISTS (
                    SELECT 1
                    FROM house_storage hs
                    WHERE hs.user_pokemon_id = up.id
              )
            ORDER BY up.is_favorite DESC, up.level DESC, up.id DESC
            LIMIT %s OFFSET %s
            """,
            (current_user["id"], limit, offset),
        )
        items = [dict(row) for row in cursor.fetchall()]

    return ok(
        {
            "items": items,
            "limit": limit,
            "offset": offset,
        }
    )


@router_v2_trade.get("/offers")
def list_trade_offers(
    scope: str = Query(default="market"),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=30, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    if scope not in {"market", "mine"}:
        fail("TRADE_SCOPE_INVALID", "El scope de trade no es valido.", 400)

    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                o.id,
                o.created_by_user_id,
                o.status,
                o.notes,
                o.expires_at,
                o.created_at,
                o.updated_at,
                up.display_name AS created_by_name,
                up.avatar_code AS created_by_avatar,
                up.trainer_team AS created_by_team
            FROM trade_offers o
            JOIN user_profiles up ON up.user_id = o.created_by_user_id
            WHERE (
                    (%s = 'market' AND o.created_by_user_id <> %s)
                 OR (%s = 'mine' AND o.created_by_user_id = %s)
              )
              AND (%s IS NULL OR o.status::text = %s)
            ORDER BY
                CASE WHEN o.status = 'open' THEN 0 ELSE 1 END,
                o.updated_at DESC,
                o.id DESC
            LIMIT %s OFFSET %s
            """,
            (
                scope,
                current_user["id"],
                scope,
                current_user["id"],
                status_filter,
                status_filter,
                limit,
                offset,
            ),
        )
        base_rows = [dict(row) for row in cursor.fetchall()]

        items = []
        for row in base_rows:
            detail = _offer_detail(cursor, row["id"], current_user["id"])
            if detail:
                items.append(detail)

    return ok(
        {
            "items": items,
            "scope": scope,
            "status": status_filter,
            "limit": limit,
            "offset": offset,
        }
    )


@router_v2_trade.get("/offers/{offer_id}")
def get_trade_offer_detail(offer_id: int, current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        detail = _offer_detail(cursor, offer_id, current_user["id"])
        if not detail:
            fail("TRADE_OFFER_NOT_FOUND", "La oferta no existe.", 404)

    return ok({"offer": detail})


@router_v2_trade.get("/transactions")
def list_trade_transactions(
    limit: int = Query(default=30, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                tt.id,
                tt.trade_offer_id,
                tt.sender_user_id,
                tt.receiver_user_id,
                tt.status,
                tt.completed_at,
                tt.created_at,
                tt.metadata,
                sender_profile.display_name AS sender_name,
                receiver_profile.display_name AS receiver_name
            FROM trade_transactions tt
            JOIN user_profiles sender_profile ON sender_profile.user_id = tt.sender_user_id
            JOIN user_profiles receiver_profile ON receiver_profile.user_id = tt.receiver_user_id
            WHERE tt.sender_user_id = %s
               OR tt.receiver_user_id = %s
            ORDER BY COALESCE(tt.completed_at, tt.created_at) DESC, tt.id DESC
            LIMIT %s OFFSET %s
            """,
            (current_user["id"], current_user["id"], limit, offset),
        )
        items = []
        for row in cursor.fetchall():
            payload = dict(row)
            payload["is_sender"] = payload["sender_user_id"] == current_user["id"]
            payload["is_receiver"] = payload["receiver_user_id"] == current_user["id"]
            items.append(payload)

    return ok({"items": items, "limit": limit, "offset": offset})


@router_v2_trade.post("/offers")
def create_trade_offer(payload: CreateTradeOfferPayload, current_user: dict = Depends(get_current_user)):
    unique_pokemon_ids = []
    for pokemon_id in payload.offered_user_pokemon_ids:
        if pokemon_id not in unique_pokemon_ids:
            unique_pokemon_ids.append(pokemon_id)

    if not unique_pokemon_ids:
        fail("TRADE_EMPTY_OFFER", "Debes ofrecer al menos un Pokemon.", 400)

    if not payload.requested_currencies:
        fail("TRADE_REQUEST_REQUIRED", "Debes indicar al menos una moneda solicitada.", 400)

    unique_currency_codes = set()
    for line in payload.requested_currencies:
        code = line.currency_code.strip().lower()
        if code in unique_currency_codes:
            fail("TRADE_DUPLICATE_CURRENCY", "No repitas monedas solicitadas en la misma oferta.", 400)
        unique_currency_codes.add(code)

    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.id,
                up.user_id,
                up.is_locked,
                up.is_trade_locked,
                up.is_team_locked,
                up.is_market_locked,
                EXISTS (
                    SELECT 1
                    FROM house_storage hs
                    WHERE hs.user_pokemon_id = up.id
                ) AS is_in_house
            FROM user_pokemon up
            WHERE up.id = ANY(%s)
            """,
            (unique_pokemon_ids,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        pokemon_by_id = {row["id"]: row for row in rows}

        if len(pokemon_by_id) != len(unique_pokemon_ids):
            fail("TRADE_INVALID_MEMBER", "Uno o mas Pokemon no existen.", 400)

        invalid_owner = [row["id"] for row in rows if row["user_id"] != current_user["id"]]
        if invalid_owner:
            fail("TRADE_INVALID_OWNER", "Solo puedes tradear Pokemon tuyos.", 400)

        blocked = [
            row["id"]
            for row in rows
            if row["is_locked"]
            or row["is_trade_locked"]
            or row["is_team_locked"]
            or row["is_market_locked"]
            or row["is_in_house"]
        ]
        if blocked:
            fail("TRADE_POKEMON_LOCKED", "Uno o mas Pokemon no estan disponibles para trade.", 400)

        currency_codes = [line.currency_code.strip().lower() for line in payload.requested_currencies]
        cursor.execute(
            """
            SELECT id, code, name
            FROM currencies
            WHERE code = ANY(%s)
              AND is_active = TRUE
            """,
            (currency_codes,),
        )
        currency_rows = [dict(row) for row in cursor.fetchall()]
        currency_by_code = {row["code"]: row for row in currency_rows}
        if len(currency_by_code) != len(currency_codes):
            fail("TRADE_CURRENCY_INVALID", "Una o mas monedas solicitadas no existen.", 400)

        notes_value = (payload.notes or "").strip() or None
        expires_at = datetime.utcnow() + timedelta(hours=payload.expires_in_hours)

        cursor.execute(
            """
            INSERT INTO trade_offers (
                created_by_user_id,
                status,
                notes,
                expires_at
            )
            VALUES (%s, 'open', %s, %s)
            RETURNING id
            """,
            (current_user["id"], notes_value, expires_at),
        )
        trade_offer_id = cursor.fetchone()["id"]

        for pokemon_id in unique_pokemon_ids:
            cursor.execute(
                """
                INSERT INTO trade_offer_pokemon (
                    trade_offer_id,
                    user_pokemon_id,
                    offered_by_user_id
                )
                VALUES (%s, %s, %s)
                """,
                (trade_offer_id, pokemon_id, current_user["id"]),
            )

        for line in payload.requested_currencies:
            code = line.currency_code.strip().lower()
            currency = currency_by_code[code]
            cursor.execute(
                """
                INSERT INTO trade_offer_currencies (
                    trade_offer_id,
                    currency_id,
                    amount,
                    is_requested
                )
                VALUES (%s, %s, %s, TRUE)
                """,
                (trade_offer_id, currency["id"], line.amount),
            )

        cursor.execute(
            """
            UPDATE user_pokemon
            SET is_trade_locked = TRUE,
                is_market_locked = TRUE,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY(%s)
            """,
            (unique_pokemon_ids,),
        )

        detail = _offer_detail(cursor, trade_offer_id, current_user["id"])

    return ok({"offer": detail})


@router_v2_trade.post("/offers/{offer_id}/cancel")
def cancel_trade_offer(offer_id: int, current_user: dict = Depends(get_current_user)):
    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT id, created_by_user_id, status
            FROM trade_offers
            WHERE id = %s
            LIMIT 1
            """,
            (offer_id,),
        )
        offer = cursor.fetchone()
        if not offer:
            fail("TRADE_OFFER_NOT_FOUND", "La oferta no existe.", 404)

        if offer["created_by_user_id"] != current_user["id"]:
            fail("TRADE_NOT_OWNER", "Solo el creador puede cancelar la oferta.", 403)

        if offer["status"] not in {"draft", "open"}:
            fail("TRADE_CANCEL_INVALID", "La oferta ya no puede cancelarse.", 400)

        cursor.execute(
            """
            UPDATE trade_offers
            SET status = 'cancelled',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (offer_id,),
        )
        _release_trade_offer_locks(cursor, offer_id)
        detail = _offer_detail(cursor, offer_id, current_user["id"])

    return ok({"offer": detail})


@router_v2_trade.post("/offers/{offer_id}/accept")
def accept_trade_offer(offer_id: int, current_user: dict = Depends(get_current_user)):
    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT
                id,
                created_by_user_id,
                status,
                expires_at
            FROM trade_offers
            WHERE id = %s
            LIMIT 1
            """,
            (offer_id,),
        )
        offer = cursor.fetchone()
        if not offer:
            fail("TRADE_OFFER_NOT_FOUND", "La oferta no existe.", 404)

        if offer["created_by_user_id"] == current_user["id"]:
            fail("TRADE_SELF_ACCEPT", "No puedes aceptar tu propia oferta.", 400)

        if offer["status"] != "open":
            fail("TRADE_NOT_OPEN", "La oferta ya no esta disponible.", 400)

        if offer["expires_at"] and offer["expires_at"] <= datetime.utcnow():
            cursor.execute(
                """
                UPDATE trade_offers
                SET status = 'expired',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (offer_id,),
            )
            _release_trade_offer_locks(cursor, offer_id)
            fail("TRADE_EXPIRED", "La oferta ya expiro.", 400)

        cursor.execute(
            """
            SELECT
                topk.user_pokemon_id,
                up.user_id,
                up.is_locked,
                up.is_trade_locked,
                up.is_team_locked,
                up.is_market_locked
            FROM trade_offer_pokemon topk
            JOIN user_pokemon up ON up.id = topk.user_pokemon_id
            WHERE topk.trade_offer_id = %s
            ORDER BY topk.id ASC
            """,
            (offer_id,),
        )
        offered_pokemon_rows = [dict(row) for row in cursor.fetchall()]
        if not offered_pokemon_rows:
            fail("TRADE_EMPTY_OFFER", "La oferta no tiene Pokemon para intercambiar.", 400)

        invalid_state = [
            row["user_pokemon_id"]
            for row in offered_pokemon_rows
            if row["user_id"] != offer["created_by_user_id"]
            or row["is_locked"]
            or not row["is_trade_locked"]
            or not row["is_market_locked"]
            or row["is_team_locked"]
        ]
        if invalid_state:
            fail("TRADE_STATE_INVALID", "La oferta ya no esta en un estado valido para aceptarse.", 400)

        cursor.execute(
            """
            SELECT
                toc.amount,
                c.code AS currency_code
            FROM trade_offer_currencies toc
            JOIN currencies c ON c.id = toc.currency_id
            WHERE toc.trade_offer_id = %s
              AND toc.is_requested = TRUE
            ORDER BY toc.id ASC
            """,
            (offer_id,),
        )
        requested_currencies = [dict(row) for row in cursor.fetchall()]
        if not requested_currencies:
            fail("TRADE_REQUEST_REQUIRED", "La oferta no tiene una solicitud valida.", 400)

        wallet_updates = []
        metadata_lines = []
        for currency_line in requested_currencies:
            receiver_wallet = _get_wallet_row(cursor, current_user["id"], currency_line["currency_code"])
            sender_wallet = _get_wallet_row(cursor, offer["created_by_user_id"], currency_line["currency_code"])
            if not receiver_wallet or not sender_wallet:
                fail("TRADE_WALLET_NOT_FOUND", "No se encontro una wallet requerida para la transaccion.", 400)

            if receiver_wallet["balance"] < currency_line["amount"]:
                fail(
                    "TRADE_INSUFFICIENT_FUNDS",
                    f"No tienes saldo suficiente en {currency_line['currency_code']}.",
                    400,
                )

            wallet_updates.append(
                {
                    "currency_code": currency_line["currency_code"],
                    "amount": currency_line["amount"],
                    "receiver_wallet": receiver_wallet,
                    "sender_wallet": sender_wallet,
                }
            )
            metadata_lines.append(
                {
                    "currency_code": currency_line["currency_code"],
                    "amount": currency_line["amount"],
                }
            )

        for update in wallet_updates:
            receiver_before = update["receiver_wallet"]["balance"]
            receiver_after = receiver_before - update["amount"]
            sender_before = update["sender_wallet"]["balance"]
            sender_after = sender_before + update["amount"]

            cursor.execute(
                """
                UPDATE user_wallets
                SET balance = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (receiver_after, update["receiver_wallet"]["id"]),
            )
            cursor.execute(
                """
                UPDATE user_wallets
                SET balance = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (sender_after, update["sender_wallet"]["id"]),
            )

            _record_wallet_transaction(
                cursor,
                wallet_id=update["receiver_wallet"]["id"],
                direction="debit",
                amount=update["amount"],
                balance_before=receiver_before,
                balance_after=receiver_after,
                source_ref=f"trade:{offer_id}",
                notes=f"Pago por trade #{offer_id}",
                metadata={
                    "trade_offer_id": offer_id,
                    "counterparty_user_id": offer["created_by_user_id"],
                    "currency_code": update["currency_code"],
                },
            )
            _record_wallet_transaction(
                cursor,
                wallet_id=update["sender_wallet"]["id"],
                direction="credit",
                amount=update["amount"],
                balance_before=sender_before,
                balance_after=sender_after,
                source_ref=f"trade:{offer_id}",
                notes=f"Ingreso por trade #{offer_id}",
                metadata={
                    "trade_offer_id": offer_id,
                    "counterparty_user_id": current_user["id"],
                    "currency_code": update["currency_code"],
                },
            )

        offered_pokemon_ids = [row["user_pokemon_id"] for row in offered_pokemon_rows]
        cursor.execute(
            """
            UPDATE user_pokemon
            SET user_id = %s,
                is_trade_locked = FALSE,
                is_market_locked = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY(%s)
            """,
            (current_user["id"], offered_pokemon_ids),
        )

        cursor.execute(
            """
            INSERT INTO trade_transactions (
                trade_offer_id,
                sender_user_id,
                receiver_user_id,
                status,
                completed_at,
                metadata
            )
            VALUES (%s, %s, %s, 'completed', CURRENT_TIMESTAMP, %s::jsonb)
            RETURNING id
            """,
            (
                offer_id,
                offer["created_by_user_id"],
                current_user["id"],
                json.dumps(
                    {
                        "pokemon_ids": offered_pokemon_ids,
                        "requested_currencies": metadata_lines,
                    }
                ),
            ),
        )
        trade_tx = dict(cursor.fetchone())

        cursor.execute(
            """
            UPDATE trade_offers
            SET status = 'accepted',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (offer_id,),
        )

        detail = _offer_detail(cursor, offer_id, current_user["id"])

    return ok({"offer": detail, "transaction": trade_tx})
