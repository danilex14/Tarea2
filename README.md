# Tarea 2 — Redis + HBase: del stream a la historia

**Integrantes:** Danilo Paredes · Felipe Contreras
**Curso:** Procesamiento Masivo de Datos (ELE051-B / EIN102B), USM 2026-2 · Profesor Diego Ramírez · Paralelo 701
**Equipo de medición:** HP Laptop 14, Intel Core i5 de 12.ª generación, 8 GB de RAM, Windows, Docker Desktop (WSL 2).

## Cómo reproducir desde cero
Requisitos: Docker Desktop (≥ 4 GB de RAM para Docker) y Python 3.10+. En Windows, usar **Git Bash** (no PowerShell). El `generador.py` del Lab 06 y el Dockerfile de HBase del Lab 07 ya están incluidos.

    bash run.sh                  # build + compose up --wait + venv + pipeline completo + verificación + preguntas + P5
    bash fallas_d1.sh            # D.1: la ingesta muere entre el put y el XACK
    bash fallas_d2.sh aof        # D.2: reinicio de Redis con AOF
    bash fallas_d2.sh none       # D.2: reinicio de Redis sin persistencia

**Cuánto tarda** (en nuestro equipo): la carga histórica 145 s, la hora en vivo 180 s (a 20x), y las preguntas y P5 un par de minutos más; en total unos 10 a 15 minutos, sin contar la primera descarga de la imagen de HBase (~340 MB).
Los resultados quedan en `resultados/` (`preguntas.json`, `p5.json`, `verificacion.txt`, `carga_historica.txt`; las salidas de D.1 y D.2 en `d1.txt`, `d2_aof.txt`, `d2_none.txt`). Los datos no se versionan: se generan con la semilla 2026.

## Resultados verificados
- Con el generador real (semilla 2026): 871.200 eventos, 17.243 repetidos y 853.953 claves distintas (histórico 846.906, en vivo 7.047), iguales a los números de control.
- `verificar.py`: 7.200 producidos, 0 pendientes y lag 0 en ambos grupos, 853.953 filas en `lecturas` y en `lecturas_tipo` → **TODO CALZA**.
- Carga histórica con batch de 5.000: 145,1 s (5.954 eventos/s, 11.908 filas/s).

## Decisiones

**Row keys (HBase)** — hora en ISO con milisegundos y offset `-03:00`, que ordena bien como bytes.
| Tabla | Clave | Rápida | Lenta |
|---|---|---|---|
| `lecturas` | `estacion#ts_medicion` | rango de una estación; último de una estación (scan inverso) | todas las estaciones en un rango (16 scans) |
| `lecturas_tipo` | `tipo#ts_medicion#estacion` | un tipo en un rango (1 scan) | una estación específica |

Idempotencia: la clave sale de la estación y de la **medición**, nunca de la llegada. Un evento repetido reescribe la misma celda con el mismo valor y una lectura tardía cae en su posición. (En los 4 choques de estación + milisegundo con distinto valor gana el último escrito.) La familia `d` tiene `VERSIONS=3` para poder mostrar en D.1 que el reproceso agrega una versión idéntica pero no una fila.

**Regiones (SPLITS)** — `lecturas`: 16 regiones, una por estación, así una consulta por estación y rango toca una sola región y la carga se reparte. `lecturas_tipo`: cortes por `tipo#día` (solo hay 3 tipos; sin el día toda la carga caería en 3 regiones y cada scan en 1).

**MAXLEN ~ 20.000** — a 40 eventos/s (20x) son ~8,3 minutos de atraso tolerable del consumidor. Se usa `~` (recorte aproximado, más barato). Una entrada recortada que nadie leyó **se pierde para ese grupo**; por eso el lag de `ingesta` debe mantenerse muy por debajo de MAXLEN.

**Persistencia de Redis: AOF con `appendfsync everysec`.** El stream es el buffer entre el productor y HBase, así que con AOF la pérdida máxima ante una caída es ~1 s de entradas, con costo de escritura bajo. Evidencia de D.2 (`docker compose restart redis` con ~3.000 eventos): con AOF sobrevivió todo (stream 3.074 → 3.074, grupos, `ultimo:*` 16, `ventana:*` 8, ranking y catálogo); sin persistencia se perdió todo (stream 3.039 → 0, sin grupos ni claves). No elegimos `always` porque 1 s de eventos de sensores es una pérdida aceptable y `always` baja el rendimiento de escritura.

**Último por medición** — `tablero.py` aplica un script Lua atómico: solo actualiza `ultimo:*` y `ranking:flujo` si `ts >= ts_guardado`, así una lectura tardía no hace retroceder el tablero. La ventana usa `score = ts de medición` y se recorta con `ZREMRANGEBYSCORE` contra el máximo ts de medición visto (`meta:ts_max`), no contra el reloj de llegada. El miembro es `ts:valor`, por lo que un repetido es el mismo miembro.

**Orden put → XACK** — en `ingesta.py` el XACK va después de que el batch de HBase se envía. Dos grupos independientes (`ingesta` y `tablero`) leen el mismo stream.

## Qué no funcionó
- Al comenzar, Docker Desktop no arrancaba porque estaban desactivados en Windows el Subsistema de Windows para Linux y la Plataforma de máquina virtual; se activaron y se reinició el equipo.
- Durante P5 el proceso de HBase se cayó (su ZooKeeper interno dejó de responder) en un equipo de 8 GB. Tras reiniciarlo comprobamos que las tablas seguían con 853.953 filas y repetimos P5 escribiendo en trozos de 500 filas con reintentos.
- El error `NOGROUP` al final de `d2_none.txt` es esperado: tras reiniciar Redis sin persistencia el stream ya no existe.

## Qué haríamos distinto
Asignar más memoria a Docker desde el comienzo (HBase necesita ~4 GB) y automatizar la espera a que las tablas estén en línea y los reintentos de escritura en todos los scripts, no solo en P5.

## Alcance de las mediciones
HBase standalone es un solo proceso con un solo RegionServer: el reparto por región se ve en los contadores de escrituras, no en latencias de cluster. Las conclusiones valen por su forma (un scan por rango lee las filas del rango, uno completo lee la tabla), no por los milisegundos absolutos. Detalle en el informe.
