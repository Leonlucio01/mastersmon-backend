from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class GoogleLoginPayload(BaseModel):
    credential: str

@router.post("/auth/google-login")
def google_login(payload: GoogleLoginPayload):
    return {
        "mensaje": "Login recibido correctamente",
        "credential_preview": payload.credential[:30] + "..."
    }