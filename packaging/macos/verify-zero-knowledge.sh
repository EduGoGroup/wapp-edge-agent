#!/usr/bin/env bash
# Guarda ZERO-KNOWLEDGE del instalador (Plan 023 · T4, R6 / ADR-0007): falla si el STAGING del .pkg contiene
# material SECRETO. En el paquete solo debe viajar el TLSCA PÚBLICO + config de bootstrap + binarios; NUNCA
# la DEK, claves privadas mTLS, ni activation codes. Se invoca desde `make pkg` ANTES de construir.
set -euo pipefail

ROOT="${1:?uso: verify-zero-knowledge.sh <dir-staging>}"
[ -d "$ROOT" ] || { echo "verify-zero-knowledge: no existe el staging $ROOT" >&2; exit 1; }

fail=0

# 1) Ninguna clave privada PEM (par mTLS o cualquier otra).
if grep -rIl -e "BEGIN [A-Z ]*PRIVATE KEY" "$ROOT" 2>/dev/null; then
	echo "ZK FAIL: hay una clave privada PEM en el paquete." >&2
	fail=1
fi

# 2) Ningún archivo de credencial/DEK por nombre (destinos que solo debe crear el enroll/Keychain local).
if find "$ROOT" -type f \( -name '*.key' -o -name 'edge.crt' -o -name 'dek' -o -name 'dek.key' -o -name '*.dek' \) 2>/dev/null | grep -q .; then
	echo "ZK FAIL: hay un archivo de credencial/clave (.key / edge.crt / dek*) en el paquete." >&2
	fail=1
fi

# 3) Ningún activation_code CON VALOR en la config de bootstrap.
if grep -rIn -E "^[[:space:]]*activation_code:[[:space:]]*['\"]?[^'\"[:space:]]" "$ROOT" 2>/dev/null; then
	echo "ZK FAIL: la config de bootstrap trae un activation_code con valor." >&2
	fail=1
fi

if [ "$fail" = "1" ]; then
	echo "Abortado: material secreto en el paquete (regresión zero-knowledge, R6 / ADR-0007)." >&2
	exit 1
fi

echo "ZK OK: el staging solo lleva binarios + bootstrap público (TLSCA/endpoint), sin secretos."
