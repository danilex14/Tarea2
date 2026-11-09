"""B.1: 864.000 eventos históricos directo a HBase con batch (sin pasar por Redis)."""
from common import *

def main():
    c = hconn(); tl, tt = c.table("lecturas"), c.table("lecturas_tipo")
    t = time.perf_counter(); n = 0
    bl, bt = tl.batch(batch_size=5000), tt.batch(batch_size=5000)
    for e in generar(0, N_HIST):
        d = fila(e)
        bl.put(k_est(e["est"], e["ts"]), d)
        bt.put(k_tipo(e["tipo"], e["ts"], e["est"]), d)
        n += 1
    bl.send(); bt.send()
    dt = time.perf_counter() - t
    msg = f"carga histórica: {n} eventos x 2 tablas en {dt:.1f} s -> {n/dt:,.0f} eventos/s ({2*n/dt:,.0f} filas/s)"
    print(msg); open("resultados/carga_historica.txt", "w").write(msg + "\n")

if __name__ == "__main__":
    main()
