"""Crea (si no existen) las tablas pre-divididas. Cortes justificados en README."""
from common import *

def lista(xs): return "[" + ",".join(f"'{x}'" for x in xs) + "]"

def crear(nombre, splits, versiones=3):
    if nombre.encode() in hconn().tables():
        print("existe", nombre); return
    shell_hbase(f"create '{nombre}', {{NAME=>'d', VERSIONS=>{versiones}}}, SPLITS=>{lista(splits)}\n")
    print("creada", nombre, "con", len(splits) + 1, "regiones")

def main():
    cat = catalogo()
    ests = sorted(cat); tipos = sorted(set(cat.values()))
    dias = ["2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07"]
    # lecturas: una región por estación (16): consultas por estación+rango tocan 1 región
    crear("lecturas", ests[1:])
    # lecturas_tipo: una región por tipo y por día (el tipo solo tiene 3 valores; sin el día el
    # scan de un tipo y rango caería en 1 región y la carga en 3)
    crear("lecturas_tipo", sorted(f"{t}#{d}" for t in tipos for d in dias))
    if os.getenv("P5") == "1":
        crear("p5_est_ts", ests[1:])
        crear("p5_ts_est", [f"{d}" for d in dias])                # pre-dividida por día
        crear("p5_salt", ["1", "2", "3"])                          # pre-dividida por salt

if __name__ == "__main__":
    main()
