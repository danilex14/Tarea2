"""B.2: la hora en vivo al stream a 20x (un evento cada 25 ms)."""
import sys
from common import *

def main(limite=N_VIVO):
    r = rconn(); t0 = time.monotonic(); n = 0
    for i, e in enumerate(generar(N_HIST, N_HIST + limite)):
        r.xadd(STREAM, {"est": e["est"], "tipo": e["tipo"], "valor": e["valor"], "ts": e["ts"]},
               maxlen=MAXLEN, approximate=True)
        n += 1; r.set("meta:producidos", n)
        espera = t0 + (i + 1) * RITMO_S - time.monotonic()
        if espera > 0: time.sleep(espera)
    r.set("meta:fin", 1)
    print(f"productor: {n} eventos en {time.monotonic()-t0:.1f} s")

if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else N_VIVO)
