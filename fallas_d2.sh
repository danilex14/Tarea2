#!/usr/bin/env bash
# D.2: uso:  ./fallas_d2.sh aof   |   ./fallas_d2.sh none     (correr las dos y comparar)
set -euo pipefail; cd "$(dirname "$0")"; { . .venv/bin/activate 2>/dev/null || . .venv/Scripts/activate; }; export PYTHONPATH="$PWD:$PWD/src"
case "${1:-aof}" in
  aof)  export REDIS_FLAGS='--appendonly yes --appendfsync everysec --save ""' ;;
  none) export REDIS_FLAGS='--appendonly no --save ""' ;;
  *) echo "uso: $0 aof|none"; exit 1 ;;
esac
docker compose rm -sf redis >/dev/null; docker volume rm -f t2_redis_data >/dev/null
docker compose up -d --wait redis
python src/setup_redis.py
python src/productor.py 7200 & PP=$!
python src/ingesta.py --consumer c1 & PI=$!
python src/tablero.py t1 & PT=$!
until [ "$(docker compose exec -T redis redis-cli get meta:producidos || echo 0)" -ge 3000 ] 2>/dev/null; do sleep 1; done
kill $PP $PI $PT 2>/dev/null || true; sleep 2
echo "=== ANTES del restart ($1)"; python src/inspeccionar.py estado; python src/inspeccionar.py pend || true
docker compose restart redis; sleep 3; docker compose up -d --wait redis
echo "=== DESPUÉS del restart ($1)"; python src/inspeccionar.py estado; python src/inspeccionar.py pend || true
