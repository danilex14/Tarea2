"""Configuración y utilidades compartidas. Si el generador del Lab 06 usa otros
nombres de campo, se ajusta SOLO aquí (CAMPOS)."""
import os, zlib, statistics, time
from datetime import datetime, timezone, timedelta
from itertools import chain, islice

import redis, happybase

TZ = timezone(timedelta(hours=-3))
T0 = "2026-11-02T00:00:00-03:00"
SEMILLA = 2026
N_HIST, N_VIVO = 864_000, 7_200
STREAM = "sensores:t2"
MAXLEN = 20_000          # ~8 min de atraso tolerado a 40 ev/s (20x). Ver README.
VENTANA_MS = 10 * 60 * 1000
RITMO_S = 0.025
G_INGESTA, G_TABLERO = "ingesta", "tablero"

CONTROL = {"hist_gen": 864_000, "vivo_gen": 7_200, "total_gen": 871_200,
           "rep_total": 17_243, "claves_total": 853_953}

# nombres posibles de cada campo en los eventos del generador
CAMPOS = {"est": ["estacion_id", "estacion"], "tipo": ["tipo_sensor"],
          "valor": ["valor", "value"], "ts": ["timestamp", "ts"]}

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
JMX_URL = os.getenv("JMX_URL", "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions")

COMUNAS = {"CC": "Concepción", "TAL": "Talcahuano", "HUA": "Hualpén", "SPP": "San Pedro de la Paz",
           "CHI": "Chiguayante", "PEN": "Penco", "COR": "Coronel"}


def rconn():
    return redis.Redis(host=REDIS_HOST, decode_responses=True)

def hconn():
    return happybase.Connection(HBASE_HOST, timeout=120000)

# ---------- tiempo ----------
def a_ms(t):
    if isinstance(t, (int, float)):
        return int(t) if t > 1e11 else int(t * 1000)
    if isinstance(t, str):
        t = datetime.fromisoformat(t)
    if t.tzinfo is None:
        t = t.replace(tzinfo=TZ)
    return int(t.timestamp()) * 1000 + t.microsecond // 1000

def iso(ms):
    return datetime.fromtimestamp(ms // 1000, TZ).replace(microsecond=(ms % 1000) * 1000).isoformat(timespec="milliseconds")

def ms_de(s):               # '2026-11-04T08:00:00-03:00' -> ms
    return a_ms(s)

# ---------- generador ----------
def _pick(ev, clave):
    for n in CAMPOS[clave]:
        if isinstance(ev, dict) and n in ev:
            return ev[n]
    raise KeyError(f"campo {clave} no encontrado en {ev}; ajustar CAMPOS en common.py")

def norm(ev):
    return {"est": str(_pick(ev, "est")), "tipo": str(_pick(ev, "tipo")),
            "valor": str(_pick(ev, "valor")), "ts": a_ms(_pick(ev, "ts"))}

def generar(desde=0, hasta=None):
    """Eventos normalizados [desde, hasta) del generador del Lab 06 con la semilla de la tarea."""
    from generador import eventos
    try:
        g = eventos(datetime.fromisoformat(T0), semilla=SEMILLA); p = next(g); g = chain([p], g)
    except (TypeError, AttributeError, ValueError):
        g = eventos(T0, semilla=SEMILLA)
    for ev in islice(g, desde, hasta):
        yield norm(ev)

def catalogo(n=5000):
    """estación -> tipo, a partir de los primeros eventos."""
    return {e["est"]: e["tipo"] for e in generar(0, n)}

def tipo_flujo(cat):
    return next(t for t in sorted(set(cat.values())) if "flujo" in t.lower())

def tipo_aire(cat):
    return next(t for t in sorted(set(cat.values())) if "aire" in t.lower() or "calidad" in t.lower())

# ---------- claves HBase ----------
def comuna(est): return COMUNAS.get(est.split("-")[0], "?")
def k_est(est, ts):  return f"{est}#{iso(ts)}"
def k_tipo(tipo, ts, est): return f"{tipo}#{iso(ts)}#{est}"
def k_ts(est, ts): return f"{iso(ts)}#{est}"
def salt(est): return zlib.crc32(est.encode()) % 4          # estable (no hash() de Python)
def k_salt(est, ts): return f"{salt(est)}#{iso(ts)}#{est}"

def fila(e):
    return {b"d:valor": e["valor"].encode(), b"d:tipo": e["tipo"].encode(),
            b"d:ts": str(e["ts"]).encode(), b"d:comuna": comuna(e["est"]).encode()}

def mediana_ms(fn, n=5):
    """Mediana de n corridas descartando la primera (calentamiento). Devuelve (ms, último resultado)."""
    fn(); ts = []; r = None
    for _ in range(n):
        t = time.perf_counter(); r = fn(); ts.append((time.perf_counter() - t) * 1000)
    return statistics.median(ts), r

def contar(conn, tabla):
    n = 0
    for _ in conn.table(tabla).scan(filter="FirstKeyOnlyFilter() AND KeyOnlyFilter()", batch_size=20000):
        n += 1
    return n

def shell_hbase(script):
    """Ejecuta comandos del shell de HBase en el contenedor (necesario para SPLITS)."""
    import subprocess
    r = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"],
                       input=script, text=True, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)
    return r.stdout
