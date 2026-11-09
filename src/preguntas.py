"""Parte C. Mediana de 5 corridas (descartando la primera). Resultados en resultados/preguntas.json"""
import json, sys, subprocess
import numpy as np
from common import *

r, hb = rconn(), hconn()
cat = catalogo(); FLUJO, AIRE = tipo_flujo(cat), tipo_aire(cat)
ESTS = sorted(cat); EF = [e for e in ESTS if cat[e] == FLUJO]
OUT = {}

def v(x): return float(x.decode() if isinstance(x, bytes) else x)

# ---------------- P1 ----------------
def p1_redis():
    p = r.pipeline()
    for e in ESTS: p.hgetall(f"ultimo:{e}")
    res = p.execute()
    return dict(zip(ESTS, res)), r.zrevrange("ranking:flujo", 0, 4, withscores=True)

def p1_hbase():
    t = hb.table("lecturas"); ult = {}
    for e in ESTS:   # último por clave = último por medición (scan inverso, limit 1)
        for k, d in t.scan(row_start=f"{e}#~", row_stop=f"{e}#", reverse=True, limit=1):
            ult[e] = {"valor": d[b"d:valor"].decode(), "ts": d[b"d:ts"].decode()}
    top = sorted(((e, float(ult[e]["valor"])) for e in EF), key=lambda x: -x[1])[:5]
    return ult, top

def p1():
    tr, (ur, topr) = mediana_ms(p1_redis); th, (uh, toph) = mediana_ms(p1_hbase)
    # último por llegada: la última entrada del stream por estación
    llegada = {}
    for _id, f in r.xrange(STREAM):
        llegada[f["est"]] = f
    difieren = {e: {"por_medicion": ur[e], "por_llegada": {"valor": llegada[e]["valor"], "ts": llegada[e]["ts"]}}
                for e in llegada if llegada[e]["ts"] != ur[e].get("ts")}
    OUT["P1"] = {"ms_redis": tr, "ms_hbase": th, "top5_redis": topr, "top5_hbase": toph,
                 "coincide_con_hbase": all(ur[e]["ts"] == uh[e]["ts"] for e in ESTS),
                 "difieren_medicion_vs_llegada": difieren,
                 "nota": "stream recortado por MAXLEN: la comparación vale para entradas aún presentes"}

# ---------------- P2 ----------------
def p2():
    anc = int(r.get("meta:ts_max"))
    def f():
        out = {}
        for e in EF:
            m = r.zrangebyscore(f"ventana:{e}", anc - VENTANA_MS, anc)
            out[e] = (sum(float(x.split(":")[1]) for x in m) / len(m), len(m)) if m else None
        return out
    t, res = mediana_ms(f)
    # memoria con y sin recorte (sin recorte: toda la hora en vivo de la estación)
    e0 = EF[0]; mem_rec = r.memory_usage(f"ventana:{e0}")
    p = r.pipeline()
    for ev in generar(N_HIST, N_HIST + N_VIVO):
        if ev["est"] == e0: p.zadd("tmp:sin_recorte", {f"{ev['ts']}:{ev['valor']}": ev["ts"]})
    p.execute(); mem_full = r.memory_usage("tmp:sin_recorte"); r.delete("tmp:sin_recorte")
    OUT["P2"] = {"ms": t, "ancla_ms": anc, "promedios_(media,n)": res,
                 "memory_usage_ventana_recortada": mem_rec, "memory_usage_hora_completa": mem_full,
                 "nota": "ventana = [ancla-10min, ancla]; sorted set por ts: rango por score en O(log N + M)"}

# ---------------- P3 ----------------
def rango_est(est, a, b, tabla="lecturas"):
    t = hb.table(tabla)
    return [(int(d[b"d:ts"]), v(d[b"d:valor"])) for _, d in
            t.scan(row_start=f"{est}#{a}", row_stop=f"{est}#{b}", columns=[b"d:ts", b"d:valor"], batch_size=5000)]

def p3():
    a, b = "2026-11-04T08:00:00.000-03:00", "2026-11-04T10:00:00.000-03:00"
    t_scan, rows = mediana_ms(lambda: rango_est("CC-001", a, b))
    def full():
        flt = "RowFilter(=, 'regexstring:^CC-001#2026-11-04T(08|09):')"
        return [(int(d[b"d:ts"]), v(d[b"d:valor"])) for _, d in
                hb.table("lecturas").scan(filter=flt, columns=[b"d:ts", b"d:valor"], batch_size=20000)]
    t_full, rows2 = mediana_ms(full, n=5)
    base = ms_de("2026-11-04T08:00:00-03:00"); cub = {}
    for ts, val in rows: cub.setdefault((ts - base) // 900000, []).append(val)
    OUT["P3"] = {"n": len(rows), "n_fullscan": len(rows2), "ms_scan_rango": t_scan, "ms_scan_total": t_full,
                 "veces_mas_lento": t_full / t_scan,
                 "prom_por_cuarto": {iso(base + k * 900000)[11:16]: sum(x) / len(x) for k, x in sorted(cub.items())}}

# ---------------- P4 ----------------
def stats(vals):   # vals: [(ts, valor)] -> promedio, máx, hora del máx (primera ocurrencia)
    vals.sort(); m = max(x for _, x in vals); ts = next(t for t, x in vals if x == m)
    return {"prom": sum(x for _, x in vals) / len(vals), "max": m, "hora_max": iso(ts)[11:23]}

def p4():
    a, b = "2026-11-05T07:00:00.000-03:00", "2026-11-05T09:00:00.000-03:00"
    def via_est():
        return {e: stats(rango_est(e, a, b)) for e in sorted(r.smembers(f"tipo:{AIRE}"))}
    def via_tipo():
        g = {}
        for k, d in hb.table("lecturas_tipo").scan(row_start=f"{AIRE}#{a}", row_stop=f"{AIRE}#{b}",
                                                   columns=[b"d:ts", b"d:valor"], batch_size=20000):
            g.setdefault(k.decode().rsplit("#", 1)[1], []).append((int(d[b"d:ts"]), v(d[b"d:valor"])))
        return {e: stats(x) for e, x in sorted(g.items())}
    ta, ra = mediana_ms(via_est); tb, rb = mediana_ms(via_tipo)
    disco = {}
    for t in ("lecturas", "lecturas_tipo"):
        try:
            o = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "sh", "-c",
                                f"du -sk /data/hbase/data/default/{t}"],
                               capture_output=True, text=True).stdout.split()
            disco[t] = f"{o[0]} KB"
        except Exception: disco[t] = "medir a mano (du en el contenedor)"
    OUT["P4"] = {"ms_a_lecturas+catalogo_redis": ta, "ms_b_lecturas_tipo": tb, "iguales": ra == rb,
                 "resultado": rb, "escrituras_extra": f"1 put adicional por evento ({N_HIST + N_VIVO:,} aprox.)",
                 "disco": disco}

# ---------------- P6 (propia) ----------------
def p6():
    """Estaciones de calidad del aire cuyo ÚLTIMO valor (Redis: el 'ahora') supera el percentil 95 de su
    HISTORIA (HBase: los 5 días previos). Redis decide el qué (catálogo + último), HBase el cuándo (referencia)."""
    inicio, fin = "2026-11-02T00:00:00.000-03:00", "2026-11-07T00:00:00.000-03:00"
    def f():
        out = {}
        for e in sorted(r.smembers(f"tipo:{AIRE}")):
            ult = float(r.hget(f"ultimo:{e}", "valor"))
            hist = np.array([x for _, x in rango_est(e, inicio, fin)])
            p95 = float(np.percentile(hist, 95))
            out[e] = {"ultimo": ult, "p95_hist": p95, "alerta": ult > p95}
        return out
    t, res = mediana_ms(f)
    OUT["P6"] = {"ms": t, "resultado": res}

if __name__ == "__main__":
    sel = sys.argv[1:] or ["p1", "p2", "p3", "p4", "p6"]
    for n in sel: globals()[n]()
    json.dump(OUT, open("resultados/preguntas.json", "w"), indent=2, ensure_ascii=False, default=str)
    print(json.dumps(OUT, indent=2, ensure_ascii=False, default=str))
