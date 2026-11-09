"""Grupo 'ingesta': stream -> HBase (2 tablas). XACK SIEMPRE después del put.
  --crash-after N : muere (os._exit) tras el put del lote N y antes del XACK (D.1)
  --recover       : primero reclama pendientes de consumidores muertos (XAUTOCLAIM)"""
import argparse, os
from common import *

def escribir(conn, msgs):
    tl, tt = conn.table("lecturas"), conn.table("lecturas_tipo")
    with tl.batch() as bl, tt.batch() as bt:       # el with envía el lote al salir
        for _id, f in msgs:
            e = {"est": f["est"], "tipo": f["tipo"], "valor": f["valor"], "ts": int(f["ts"])}
            d = fila(e)
            bl.put(k_est(e["est"], e["ts"]), d)
            bt.put(k_tipo(e["tipo"], e["ts"], e["est"]), d)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--consumer", default="c1"); ap.add_argument("--crash-after", type=int, default=0)
    ap.add_argument("--recover", action="store_true"); ap.add_argument("--lote", type=int, default=200)
    a = ap.parse_args()
    r, hb = rconn(), hconn(); lotes = 0

    if a.recover:
        cursor = "0-0"
        while True:
            res = r.xautoclaim(STREAM, G_INGESTA, a.consumer, min_idle_time=0, start_id=cursor, count=a.lote)
            cursor, msgs = res[0], res[1]
            if msgs:
                escribir(hb, msgs); r.xack(STREAM, G_INGESTA, *[m[0] for m in msgs])
                print(f"[{a.consumer}] recuperadas y escritas {len(msgs)} entradas pendientes")
            if cursor in ("0-0", b"0-0"): break

    while True:
        res = r.xreadgroup(G_INGESTA, a.consumer, {STREAM: ">"}, count=a.lote, block=1000)
        if not res:
            if r.get("meta:fin"): break
            continue
        msgs = res[0][1]
        escribir(hb, msgs)                      # 1) put
        lotes += 1
        if a.crash_after and lotes == a.crash_after:
            print(f"[{a.consumer}] MUERE tras el put del lote {lotes} ({len(msgs)} entradas) y antes del XACK")
            os._exit(1)
        r.xack(STREAM, G_INGESTA, *[m[0] for m in msgs])    # 2) ack
    print(f"[{a.consumer}] ingesta terminada ({lotes} lotes)")

if __name__ == "__main__":
    main()
