import base64
import json
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ALGORITHM = "dir"
ENCRYPTION = "A256GCM"
NESTED_CONTENT_TYPE = "JWT"
IV_BYTES = 12
TAG_BYTES = 16


class InvalidEncryptedToken(ValueError):
    pass


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def load_encryption_key(name: str = "JWT_ENCRYPTION_KEY") -> bytes:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno {name}")
    try:
        key = _b64url_decode(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} no es base64url valido") from exc
    if len(key) != 32 or _b64url_encode(key) != value:
        raise RuntimeError(f"{name} debe ser una clave de 32 bytes en base64url sin relleno (43 caracteres)")
    return key


def encrypt_nested_jwt(signed_token: str, key: bytes) -> str:
    header = _b64url_encode(
        json.dumps({"alg": ALGORITHM, "enc": ENCRYPTION, "cty": NESTED_CONTENT_TYPE}, separators=(",", ":")).encode()
    )
    iv = os.urandom(IV_BYTES)
    sealed = AESGCM(key).encrypt(iv, signed_token.encode(), header.encode("ascii"))
    ciphertext, tag = sealed[:-TAG_BYTES], sealed[-TAG_BYTES:]
    return ".".join([header, "", _b64url_encode(iv), _b64url_encode(ciphertext), _b64url_encode(tag)])


def decrypt_nested_jwt(token: str, key: bytes) -> str:
    parts = token.split(".")
    if len(parts) != 5:
        raise InvalidEncryptedToken("El token no es un JWE compacto")
    encoded_header, encrypted_key, encoded_iv, encoded_ciphertext, encoded_tag = parts
    try:
        header = json.loads(_b64url_decode(encoded_header))
        iv = _b64url_decode(encoded_iv)
        ciphertext = _b64url_decode(encoded_ciphertext)
        tag = _b64url_decode(encoded_tag)
    except ValueError as exc:
        raise InvalidEncryptedToken("El JWE esta mal formado") from exc
    if (
        not isinstance(header, dict)
        or header.get("alg") != ALGORITHM
        or header.get("enc") != ENCRYPTION
        or header.get("cty") != NESTED_CONTENT_TYPE
        or "zip" in header
        or encrypted_key != ""
        or len(iv) != IV_BYTES
        or len(tag) != TAG_BYTES
    ):
        raise InvalidEncryptedToken("Cabecera JWE no soportada")
    try:
        return AESGCM(key).decrypt(iv, ciphertext + tag, encoded_header.encode("ascii")).decode()
    except (InvalidTag, UnicodeDecodeError) as exc:
        raise InvalidEncryptedToken("No se pudo descifrar el token") from exc
