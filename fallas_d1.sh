#!/usr/bin/env bash
# D.1: la ingesta muere entre el put y el XACK; otro consumidor recupera; HBase queda exacto.
set -euo pipefail; cd "$(dirname "$0")"; { . .venv/bin/activate 2>/dev/null || . .venv/Scripts/activate; }; export PYTHONPATH="$PWD:$PWD/src"
python src/setup_redis.py
python src/productor.py & PP=$!
python src/tablero.py t1 & PT=$!
python src/ingesta.py --consumer c1 --crash-after 3 || echo "c1 murió (código $?)"
echo "--- 1) qué quedó pendiente"; python src/inspeccionar.py pend
echo "--- 2) otro consumidor recupera (XAUTOCLAIM) y sigue"
python src/ingesta.py --consumer c2 --recover
wait $PP $PT
echo "--- 3) HBase vs números de control"; python src/verificar.py
echo "--- 4) versiones en las celdas reprocesadas"; python src/inspeccionar.py versiones
