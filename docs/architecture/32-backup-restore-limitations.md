# 32 - Limitaciones del Backup/Restore en Zeebe

## Contexto

Zeebe es un sistema de **estado presente**: mantiene en memoria y disco solo el estado actual de las instancias de proceso, jobs, variables, etc. Todo lo que va sucediendo (eventos, comandos) se exporta a sistemas externos (Elasticsearch, OpenSearch) y luego se compacta del journal. Esto tiene implicaciones importantes para el backup y restore.

## Cómo funciona el Backup

El backup es **manual/on-demand** — no hay backups automáticos periódicos. Se dispara vía API:

```
takeBackup(backupId, checkpointPosition)
```

Para cada partición:

1. Se toma un **snapshot** del estado actual (RocksDB checkpoint, instantáneo gracias a copy-on-write)
2. Se recolectan los **segmentos del journal** (el log de Raft que aún no se ha compactado)
3. Se sube todo a almacenamiento externo (S3, GCS, Azure)

**No hay programación automática.** La frecuencia depende de tu operativa.

## Qué guarda un Backup

| Dato | ¿Incluido? | Detalle |
|------|------------|---------|
| Estado RocksDB completo | ✅ | Procesos desplegados, instancias activas, jobs pendientes, variables, timers, message subscriptions |
| Posiciones de exporters | ✅ | Guardadas en ExportersState (dentro de RocksDB) |
| Segmentos del journal | ✅ | Solo los que no han sido compactados |
| Records ya exportados y compactados | ❌ | Ya borrados del journal |
| Historial completo de eventos | ❌ | Eso vive en Elasticsearch/OpenSearch |

### Formato del snapshot

El ID del snapshot codifica las posiciones clave:

```
{index}-{term}-{timestamp}-{processedPosition}-{exportedPosition}
```

La `exportedPosition` indica hasta dónde se ha exportado — los records por debajo de esa posición ya pueden haberse compactado del journal.

## El problema: restaurar un backup antiguo

### Lo que pasa al restaurar

Cuando restauras un backup de hace N tiempo:

1. **Se recupera el estado de hace N tiempo** — instancias de proceso, jobs, variables, todo como estaba en ese momento
2. **Se pierde todo lo que pasó después** — nuevas instancias, jobs completados, mensajes recibidos, variables modificadas
3. **Los exporters re-exportan** desde su posición guardada (at-least-once), generando **duplicados** en Elasticsearch/OpenSearch

### Consecuencias concretas

| Situación | Resultado tras restore |
|-----------|----------------------|
| Proceso completado después del backup | Se "resucita" y se vuelve a ejecutar desde donde estaba en el momento del backup |
| Job completado después del backup | Se re-activa, los workers lo recibirán de nuevo |
| Variables modificadas después del backup | Vuelven al valor del momento del backup |
| Mensajes recibidos después del backup | Se pierden, hay que re-enviarlos |
| Nuevas instancias creadas después del backup | No existen, hay que crearlas de nuevo |
| Datos en Elasticsearch/OpenSearch | Inconsistentes — duplicados + datos de instancias que Zeebe ya no tiene |

### Diagrama temporal

```
Backup tomado          Fallo del sistema
    │                       │
    ▼                       ▼
────┬───────────────────────┬──────── tiempo
    │                       │
    │  ← Este periodo se    │
    │    pierde al restore  │
    │                       │
    │  - 50 instancias      │
    │    completadas         │
    │  - 200 jobs ejecutados│
    │  - 30 mensajes        │
    │    recibidos           │
    │                       │
    ▼                       │
  Estado que               Estado que
  se restaura              se perdió
```

## Cuándo es útil un Restore

### Útil (minutos desde el backup)
- Fallo catastrófico del clúster (discos, hardware)
- Corrupción de datos
- El impacto es bajo porque poco ha cambiado

### Problemático (horas/días desde el backup)
- Mucha actividad entre backup y fallo
- Procesos completados se re-ejecutarán (efectos secundarios duplicados)
- Inconsistencia severa con sistemas externos

### Nunca útil para
- Recuperar datos históricos (esos están en Elasticsearch/OpenSearch)
- "Volver atrás" un proceso individual
- Auditoría (Zeebe no guarda historial, es solo estado presente)

## Flujo de Recovery al Arrancar

```
1. Cargar snapshot (incluye posiciones de exporters)
   ↓
2. ExporterDirector obtiene última posición por exporter
   ↓
3. La mínima posición entre todos los exporters = punto de inicio
   ↓
4. LogStreamReader.seek(mínima posición + 1)
   ↓
5. Replay journal desde snapshot → estado reconstruido
   ↓
6. Re-exportar records desde punto de inicio (at-least-once)
```

## Recomendaciones

1. **Backups frecuentes** — cuanto más frecuentes, menor la ventana de pérdida
2. **Workers idempotentes** — diseña los workers para que re-ejecutar un job no cause problemas (clave para tolerar restores)
3. **Monitorizar la exportación** — si los exporters se atrasan, el journal crece y el backup incluye más datos, pero el restore también es más lento
4. **No depender solo de Zeebe para historial** — los datos históricos deben estar en Elasticsearch/OpenSearch con sus propios backups
5. **Probar el restore periódicamente** — en un entorno de staging, no en producción

## Relación con otros documentos

- [11-snapshots-journal.md](11-snapshots-journal.md) — Detalle de cómo funcionan snapshots y journal
- [14-exporters.md](14-exporters.md) — Cómo los exporters gestionan posiciones y re-exportan
- [17-backup-restore.md](17-backup-restore.md) — API y proceso técnico de backup/restore
- [09-state-zb-db.md](09-state-zb-db.md) — Estado persistente en RocksDB
