#!/usr/bin/env bash
# Pipeline completo de punta a punta. Requisitos: docker y python3 (generador.py del Lab 06 ya incluido).
set -euo pipefail
cd "$(dirname "$0")"
T0=$(date +%s); mkdir -p resultados
[ -f generador.py ] || { echo "Falta generador.py del Lab 06 en la raíz del repo"; exit 1; }

docker compose build
docker compose up -d --wait
[ -d .venv ] || (python3 -m venv .venv 2>/dev/null || python -m venv .venv)
. .venv/bin/activate 2>/dev/null || . .venv/Scripts/activate
pip install -q -r requirements.txt
export PYTHONPATH="$PWD:$PWD/src"

python src/setup_hbase.py
python src/cargar_historico.py                       # B.1
python src/setup_redis.py                            # limpia Redis y crea los dos grupos
python src/productor.py &       P1=$!                # B.2 (a 20x: ~3 min)
python src/ingesta.py --consumer c1 &  P2=$!
python src/tablero.py t1 &      P3=$!
wait $P1 $P2 $P3
python src/verificar.py | tee resultados/verificacion.txt   # B.3
python src/preguntas.py > resultados/preguntas.log           # Parte C (P1-P4, P6)
P5=1 python src/p5_hotspot.py | tee resultados/p5.log        # P5
echo "Total: $(( $(date +%s) - T0 )) s"
