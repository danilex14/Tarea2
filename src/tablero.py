"""Grupo 'tablero': mantiene en Redis el estado de ahora.
'Último' = último por MEDICIÓN: el script Lua compara ts con el guardado y solo avanza si ts >= actual
(atómico). Una lectura tardía no hace retroceder ultimo:* ni ranking:flujo. La ventana se ancla al
máximo ts de medición visto (meta:ts_max), no al reloj de llegada."""
import sys
from common import *

LUA = """
local ts = tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], 'ts')
if (not cur) or ts >= tonumber(cur) then
  redis.call('HSET', KEYS[1], 'valor', ARGV[2], 'ts', ARGV[3], 'tipo', ARGV[4])
  if ARGV[5] == '1' then redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]) end
end
local mx = redis.call('GET', KEYS[4])
if (not mx) or ts > tonumber(mx) then redis.call('SET', KEYS[4], ARGV[3]); mx = ARGV[3] end
if ARGV[5] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[3] .. ':' .. ARGV[2])
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. string.format('%d', tonumber(mx) - tonumber(ARGV[6])))
end
return 1
"""

def main(consumer="t1", lote=200):
    r = rconn(); script = r.register_script(LUA)
    flujo = tipo_flujo(catalogo())
    while True:
        res = r.xreadgroup(G_TABLERO, consumer, {STREAM: ">"}, count=lote, block=1000)
        if not res:
            if r.get("meta:fin"): break
            continue
        msgs = res[0][1]; p = r.pipeline(transaction=False)
        for _id, f in msgs:
            es = "1" if f["tipo"] == flujo else "0"
            p.sadd(f"tipo:{f['tipo']}", f["est"])
            script(keys=[f"ultimo:{f['est']}", "ranking:flujo", f"ventana:{f['est']}", "meta:ts_max"],
                   args=[f["est"], f["valor"], f["ts"], f["tipo"], es, VENTANA_MS], client=p)
        p.execute()
        r.xack(STREAM, G_TABLERO, *[m[0] for m in msgs])
    print("tablero terminado")

if __name__ == "__main__":
    main(*(sys.argv[1:2] or ["t1"]))
