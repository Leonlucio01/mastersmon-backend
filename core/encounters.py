from datetime import datetime, timedelta, timezone
from threading import Lock
from uuid import uuid4


ENCOUNTER_TTL_MINUTES = 10
_LOCK = Lock()
_ACTIVE_ENCOUNTERS: dict[str, dict] = {}


def _utcnow():
    return datetime.now(timezone.utc)


def cleanup_expired_encounters():
    now = _utcnow()
    with _LOCK:
        expired = [
            encounter_id
            for encounter_id, payload in _ACTIVE_ENCOUNTERS.items()
            if payload["expires_at"] <= now
        ]
        for encounter_id in expired:
            _ACTIVE_ENCOUNTERS.pop(encounter_id, None)


def create_encounter(payload: dict) -> dict:
    cleanup_expired_encounters()
    encounter_id = str(uuid4())
    full_payload = dict(payload)
    full_payload["id"] = encounter_id
    full_payload["created_at"] = _utcnow()
    full_payload["expires_at"] = _utcnow() + timedelta(minutes=ENCOUNTER_TTL_MINUTES)

    with _LOCK:
        _ACTIVE_ENCOUNTERS[encounter_id] = full_payload

    return full_payload


def get_encounter(encounter_id: str) -> dict | None:
    cleanup_expired_encounters()
    with _LOCK:
        payload = _ACTIVE_ENCOUNTERS.get(encounter_id)
        return dict(payload) if payload else None


def delete_encounter(encounter_id: str):
    with _LOCK:
        _ACTIVE_ENCOUNTERS.pop(encounter_id, None)
