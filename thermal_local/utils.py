from typing import Optional, List, Dict, Any
import bcrypt

BCRYPT_MAX_PASSWORD_BYTES = 72


def _to_bcrypt_bytes(password: str) -> bytes:
    raw = password.encode("utf-8")
    return raw[:BCRYPT_MAX_PASSWORD_BYTES] if len(raw) > BCRYPT_MAX_PASSWORD_BYTES else raw


class Hasher:
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return bcrypt.checkpw(
            _to_bcrypt_bytes(plain_password),
            hashed_password.encode("utf-8"),
        )

    @staticmethod
    def get_password_hash(password: str) -> str:
        return bcrypt.hashpw(
            _to_bcrypt_bytes(password),
            bcrypt.gensalt(rounds=12),
        ).decode("utf-8")