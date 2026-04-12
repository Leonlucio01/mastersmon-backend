from fastapi import HTTPException, status


def ok(data):
    return {"ok": True, "data": data}


def fail(code: str, message: str, status_code: int = status.HTTP_400_BAD_REQUEST):
    raise HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
        },
    )
