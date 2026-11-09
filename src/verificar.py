"""B.3: compara lo que quedó con los números de control."""
from common import *

def main():
    r, hb = rconn(), hconn(); ok = True
    prod = int(r.get("meta:producidos") or 0)
    print(f"eventos producidos: {prod} (esperado {N_VIVO})"); ok &= prod == N_VIVO
    print(f"XLEN del stream (recortado con MAXLEN~{MAXLEN}): {r.xlen(STREAM)}")
    for g in r.xinfo_groups(STREAM):
        print(f"grupo {g['name']}: entradas leídas={g.get('entries-read')} pendientes={g['pending']} lag={g.get('lag')}")
        ok &= g["pending"] == 0
    for t in ("lecturas", "lecturas_tipo"):
        n = contar(hb, t); print(f"filas en {t}: {n} (control {CONTROL['claves_total']})")
        if n != CONTROL["claves_total"]:
            ok = False
            print(f"  DIFERENCIA {CONTROL['claves_total']-n}: revisar historia/vivo/colisiones (17.243 repetidos + 4 choques ya descontados)")
    print("RESULTADO:", "TODO CALZA" if ok else "HAY DIFERENCIAS (ver arriba)")

if __name__ == "__main__":
    main()
