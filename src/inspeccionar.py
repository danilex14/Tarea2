"""Ayudas para D.1 / D.2:  pend | versiones | estado"""
import sys
from common import *
r = rconn(); m = sys.argv[1]
if m == "pend":
    print("XPENDING ingesta:", r.xpending(STREAM, G_INGESTA))
    for g in r.xinfo_groups(STREAM): print(g["name"], "pendientes:", g["pending"], "consumidores:", g.get("consumers"))
    print([ (c["name"], c["pending"]) for c in r.xinfo_consumers(STREAM, G_INGESTA)])
elif m == "versiones":      # celdas con más de una versión (lote reprocesado) -> evidencia de idempotencia
    hb = hconn(); t = hb.table("lecturas"); n = 0
    for _id, f in r.xrange(STREAM, count=400):
        k = k_est(f["est"], int(f["ts"]))
        cel = t.cells(k, b"d:valor", versions=5)
        if len(cel) > 1:
            print(k, "-> versiones:", cel); n += 1
            if n >= 3: break
    print("celdas con >1 versión (muestra):", n)
elif m == "estado":
    print("XLEN", r.xlen(STREAM), "| grupos", [(g["name"], g["pending"]) for g in r.xinfo_groups(STREAM)] if r.exists(STREAM) else "sin stream")
    print("ultimo:*", len(r.keys("ultimo:*")), "| ventana:*", len(r.keys("ventana:*")), "| ranking:", r.zcard("ranking:flujo"),
          "| tipo:*", len(r.keys("tipo:*")), "| producidos", r.get("meta:producidos"))
