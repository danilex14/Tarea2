"""P5: la hora en vivo en 3 tablas (distinta clave), escrituras por región (JMX) y tiempos de lectura."""
import json, re, urllib.request
from common import *

TABS = {"p5_est_ts": k_est, "p5_ts_est": k_ts, "p5_salt": k_salt}

def jmx():
    """{(tabla, start_key de la región): writeRequestCount acumulado} (mismo método del Lab 07, 3.2)"""
    m = json.load(urllib.request.urlopen(JMX_URL))["beans"][0]; hb = hconn(); out = {}
    for t in TABS:
        for rg in hb.table(t).regions():
            enc = rg["name"].decode().split(".")[-2]
            out[(t, rg["start_key"].decode() or "(inicio)")] = m.get(
                f"Namespace_default_table_{t}_region_{enc}_metric_writeRequestCount", 0)
    return out

def main():
    existentes = {t.decode() for t in hconn().tables()}
    for t in TABS:
        if t in existentes:
            shell_hbase(f"disable '{t}'\ndrop '{t}'\n")
    os.environ["P5"] = "1"; import setup_hbase; setup_hbase.main()
    for t in TABS:                                   # esperar a que las regiones estén en línea
        for _ in range(30):
            try: hconn().table(t).row(b"x"); break
            except Exception: time.sleep(2)
    hb = hconn(); ev = list(generar(N_HIST, N_HIST + N_VIVO))
    time.sleep(12); antes = jmx()
    def escribir(tabla, kf):
        for i in range(0, len(ev), 500):
            for intento in range(4):
                try:
                    with hconn().table(tabla).batch() as b:
                        for e in ev[i:i + 500]: b.put(kf(e["est"], e["ts"]), fila(e))
                    break
                except Exception as ex:
                    print("reintento", tabla, i, repr(ex)); time.sleep(5)
            else:
                raise RuntimeError(f"no se pudo escribir en {tabla}")
    for tabla, kf in TABS.items():
        escribir(tabla, kf); print("escrita", tabla)
    time.sleep(15); despues = jmx(); hb = hconn()          # JMX publica cada ~10 s
    tabla_reg = {}
    for (t, reg), val in despues.items():
        tabla_reg.setdefault(t, []).append(val - antes.get((t, reg), 0))
    print("escrituras por región (ordenadas):")
    for t in TABS: print(f"  {t:10s}", sorted(tabla_reg.get(t, []), reverse=True))

    a, b = ms_de("2026-11-07T00:10:00-03:00"), ms_de("2026-11-07T00:20:00-03:00")
    ia, ib = iso(a), iso(b); ests = sorted(catalogo())
    def sc(t, s, e): return sum(1 for _ in hb.table(t).scan(row_start=s, row_stop=e, columns=[b"d:valor"], batch_size=5000))
    dis = {
        "estacion#ts (16 rangos)": lambda: sum(sc("p5_est_ts", f"{e}#{ia}", f"{e}#{ib}") for e in ests),
        "ts#estacion (1 rango)": lambda: sc("p5_ts_est", ia, ib),
        "salt#ts#estacion (4 rangos)": lambda: sum(sc("p5_salt", f"{s}#{ia}", f"{s}#{ib}") for s in range(4)),
    }
    res = {}
    for n, f in dis.items():
        t, c = mediana_ms(f); res[n] = {"ms": t, "filas": c}; print(f"{n:30s} {t:8.1f} ms  {c} filas")
    json.dump({"regiones": tabla_reg, "lectura": res}, open("resultados/p5.json", "w"), indent=2)

if __name__ == "__main__":
    main()
