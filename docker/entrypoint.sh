#!/bin/sh
set -e

echo "[ENTRYPOINT] Attente de la disponibilité de la base de données..."
if [ -n "$DB_HOST" ] && [ -n "$DB_PORT" ]; then
    while ! nc -z "$DB_HOST" "$DB_PORT"; do
        echo "[ENTRYPOINT] En attente de $DB_HOST:$DB_PORT..."
        sleep 1
    done
    echo "[ENTRYPOINT] Base de données disponible !"
fi

echo "[ENTRYPOINT] Lancement de l'application : $@"
exec "$@"
