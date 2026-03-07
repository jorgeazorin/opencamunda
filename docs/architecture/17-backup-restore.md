# 17 - Backup y Restore

## Propósito

El sistema de backup permite hacer copias de seguridad del estado completo de Zeebe a almacenamiento externo (S3, GCS, Azure) y restaurarlas.

## Módulos

```
zeebe/backup/          ← API e interfaces de backup
zeebe/backup-stores/
├── gcs/               ← Google Cloud Storage
├── s3/                ← AWS S3
└── azure/             ← Azure Blob Storage
zeebe/restore/         ← Proceso de restauración
```

## API de Backup

### BackupStore (Interface Central)

```java
public interface BackupStore {
    CompletableFuture<Void> save(Backup backup);
    CompletableFuture<Backup> restore(BackupIdentifier id, Path targetFolder);
    CompletableFuture<BackupStatus> getStatus(BackupIdentifier id);
    CompletableFuture<Collection<BackupStatus>> list(BackupIdentifierWildcard wildcard);
    CompletableFuture<Void> delete(BackupIdentifier id);
    CompletableFuture<Void> markFailed(BackupIdentifier id, String failureReason);
}
```

### BackupManager (Por Partición)

```java
public interface BackupManager {
    void takeBackup(long backupId, long checkpointPosition);
    CompletableFuture<BackupStatus> getBackupStatus(long backupId);
    CompletableFuture<Collection<BackupStatus>> listBackups();
}
```

### Backup (Datos)

```java
public interface Backup {
    BackupDescriptor descriptor();  // Metadatos
    NamedFileSet snapshot();         // Archivos del snapshot
    NamedFileSet segments();         // Segmentos del journal
}

public interface BackupDescriptor {
    SnapshotId snapshotId();
    long checkpointPosition();
    int numberOfPartitions();
    String brokerVersion();
}
```

## Proceso de Backup

```
1. Trigger: API request → takeBackup(backupId, checkpointPosition)
   ↓
2. Para cada partición:
   ├─ Tomar snapshot del estado actual
   ├─ Reservar en estado interno
   └─ Recolectar archivos de segmentos del journal
   ↓
3. Crear Backup object:
   ├─ descriptor: ID, posición, versión
   ├─ snapshot: archivos .sst, metadata, checksums
   └─ segments: archivos del journal
   ↓
4. BackupStore.save(backup)
   ├─ Subir archivos a almacenamiento externo
   └─ Actualizar manifest a COMPLETED
   ↓
5. Estado: IN_PROGRESS → COMPLETED (o FAILED si error)
```

## Proceso de Restore

```
1. Obtener manifest: BackupStore.getStatus(backupId)
   ↓
2. Validar: estado debe ser COMPLETED
   ↓
3. BackupStore.restore(backupId, targetFolder)
   ├─ Descargar snapshot files
   └─ Descargar segment files
   ↓
4. Restablecer estado:
   ├─ Snapshot → directorio de snapshots del partition
   └─ Segments → directorio de journal del partition
   ↓
5. Broker arranca normalmente:
   ├─ Carga snapshot
   ├─ Replay journal desde snapshot
   └─ Estado restaurado
```

## Implementaciones de BackupStore

### S3 (AWS)

```
Clase: S3BackupStore
- Cliente: AWS S3 async client
- Paralelismo: hasta 16 scans paralelos
- Organización: backups/{backupId}/partition-{id}/snapshots/ y segments/
```

### GCS (Google Cloud)

```
Clase: GcsBackupStore
- Cliente: Google Cloud Storage client
- Organización similar a S3
```

### Azure Blob Storage

```
Clase: AzureBackupStore
- Cliente: Azure Blob Storage client
- Usa virtual threads para operaciones
```

## Estructura en Storage Externo

```
bucket/
└── backups/
    └── backup-42/
        ├── manifest.json           ← Estado y metadatos
        ├── partition-1/
        │   ├── snapshots/
        │   │   ├── 000001.sst
        │   │   ├── MANIFEST-000001
        │   │   └── zeebe.metadata
        │   └── segments/
        │       ├── journal-5.log
        │       └── journal-6.log
        ├── partition-2/
        │   └── ...
        └── partition-3/
            └── ...
```

## Estados del Backup

```
IN_PROGRESS → COMPLETED    (éxito)
IN_PROGRESS → FAILED       (error)
```

## Configuración

```yaml
zeebe:
  broker:
    data:
      backup:
        store: S3    # S3 | GCS | AZURE
        s3:
          bucketName: "zeebe-backups"
          region: "us-east-1"
          endpoint: ""
          accessKey: ""
          secretKey: ""
```

