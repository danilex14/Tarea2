"""Deja Redis limpio y crea los dos grupos (cada uno ve todo el stream)."""
from common import *
r = rconn(); r.flushall()
for g in (G_INGESTA, G_TABLERO):
    r.xgroup_create(STREAM, g, id="0", mkstream=True)
print("redis limpio, grupos:", [g["name"] for g in r.xinfo_groups(STREAM)])
