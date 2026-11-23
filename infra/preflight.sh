#!/bin/sh
# Guardrail S2: refuse to start the stack with placeholder or weak passwords.
# Compares each password with its value in .env.example. Prints names only, never values.
set -eu

EXAMPLE=/preflight/env.example
MIN_LENGTH=12
fail=0

placeholder() {
  # value of NAME=... in .env.example (first match; empty if absent)
  sed -n "s/^$1=//p" "$EXAMPLE" | head -n 1
}

for name in POSTGRES_PASSWORD PIPELINE_WRITER_PASSWORD READONLY_PASSWORD GRAFANA_ADMIN_PASSWORD; do
  value=$(printenv "$name" || true)
  if [ -z "$value" ]; then
    echo "preflight: $name is empty; set it in .env" >&2
    fail=1
  elif [ "$value" = "$(placeholder "$name")" ]; then
    echo "preflight: $name still has the .env.example placeholder; set a real password in .env" >&2
    fail=1
  elif [ "${#value}" -lt "$MIN_LENGTH" ]; then
    echo "preflight: $name is shorter than $MIN_LENGTH characters" >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "preflight: FAILED, stack not started" >&2
  exit 1
fi
echo "preflight: passwords set, none is a placeholder"
