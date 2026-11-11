import base64
import json

import pytest

from app.jwe import InvalidEncryptedToken, decrypt_nested_jwt, encrypt_nested_jwt

KEY = bytes([1]) * 32
SIGNED = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c3VhcmlvQGV4YW1wbGUuY29tIn0.firma"


def decode_segment(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def test_cifra_en_cinco_segmentos_con_la_cabecera_esperada():
    token = encrypt_nested_jwt(SIGNED, KEY)
    header, encrypted_key, _, ciphertext, _ = token.split(".")

    assert json.loads(decode_segment(header)) == {"alg": "dir", "enc": "A256GCM", "cty": "JWT"}
    assert encrypted_key == ""
    assert b"usuario@example.com" not in decode_segment(ciphertext)


def test_descifra_lo_que_cifra():
    assert decrypt_nested_jwt(encrypt_nested_jwt(SIGNED, KEY), KEY) == SIGNED


def test_rechaza_otra_clave():
    with pytest.raises(InvalidEncryptedToken):
        decrypt_nested_jwt(encrypt_nested_jwt(SIGNED, bytes([2]) * 32), KEY)


def test_rechaza_contenido_alterado():
    parts = encrypt_nested_jwt(SIGNED, KEY).split(".")
    parts[3] = ("A" if parts[3][0] != "A" else "B") + parts[3][1:]
    with pytest.raises(InvalidEncryptedToken):
        decrypt_nested_jwt(".".join(parts), KEY)


def test_rechaza_un_jws_plano():
    with pytest.raises(InvalidEncryptedToken):
        decrypt_nested_jwt(SIGNED, KEY)
