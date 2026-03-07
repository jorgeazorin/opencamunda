# OpenCamunda - Documentación de Arquitectura

> Fork de Camunda Zeebe v8.5.25 (última versión con licencia comunitaria libre)

## Propósito

Esta documentación tiene como objetivo que **cualquier persona o agente de IA** pueda entender cómo funciona el proyecto desde cero. Cada documento explica un área funcional del sistema con el nivel de detalle necesario para poder modificar el código con confianza.

## Índice de Documentos

### Nivel 0 - Visión General
- [00-project-overview.md](00-project-overview.md) - Qué es el proyecto, estructura general, cómo compilar y ejecutar
- [01-architecture-overview.md](01-architecture-overview.md) - Arquitectura de alto nivel, flujo de datos, componentes principales

### Nivel 1 - Módulos Core
- [02-broker.md](02-broker.md) - El servidor Zeebe: arranque, particiones, clustering, ciclo de vida
- [03-engine.md](03-engine.md) - Motor de ejecución BPMN: procesamiento de comandos, estado, BPMN elements
- [04-gateway.md](04-gateway.md) - Gateway gRPC y REST: API pública, interceptores, routing
- [05-protocol.md](05-protocol.md) - Protocolo binario: SBE, records, serialización de comandos y eventos

### Nivel 2 - Infraestructura
- [06-atomix-raft.md](06-atomix-raft.md) - Consenso distribuido: Raft, membership, replicación
- [07-logstreams.md](07-logstreams.md) - Log append-only: escritura, lectura, posiciones lógicas
- [08-stream-platform.md](08-stream-platform.md) - Stream processing: procesamiento de eventos, command/event pattern
- [09-state-zb-db.md](09-state-zb-db.md) - Estado persistente: RocksDB, column families, transacciones
- [10-scheduler.md](10-scheduler.md) - Actor scheduler: modelo de actores, threads, planificación de tareas
- [11-snapshots-journal.md](11-snapshots-journal.md) - Snapshots y journaling: persistencia, recuperación, compactación

### Nivel 3 - Funcionalidades
- [12-bpmn-execution.md](12-bpmn-execution.md) - Ejecución BPMN: cómo se ejecuta cada elemento BPMN
- [13-dmn-feel.md](13-dmn-feel.md) - DMN y FEEL: evaluación de decisiones y expresiones
- [14-exporters.md](14-exporters.md) - Exporters: Elasticsearch, OpenSearch, API para exporters custom
- [15-jobs-workers.md](15-jobs-workers.md) - Sistema de jobs: activación, completado, streaming, workers
- [16-messaging.md](16-messaging.md) - Mensajes: correlación, publicación, subscripciones
- [17-backup-restore.md](17-backup-restore.md) - Backup y restore: S3, GCS, Azure, proceso de backup

### Nivel 4 - Clientes y API
- [18-java-client.md](18-java-client.md) - Cliente Java: API, comandos, configuración
- [19-go-client-zbctl.md](19-go-client-zbctl.md) - Cliente Go y zbctl: CLI, comandos, workers
- [20-spring-boot-sdk.md](20-spring-boot-sdk.md) - Spring Boot starter: autoconfiguración, anotaciones

### Nivel 5 - Operaciones y Testing
- [21-topology-clustering.md](21-topology-clustering.md) - Topología: distribución de particiones, descubrimiento
- [22-transport-networking.md](22-transport-networking.md) - Red: transporte, comunicación entre nodos
- [23-auth-identity.md](23-auth-identity.md) - Autenticación: OAuth, Identity, interceptores
- [24-testing-qa.md](24-testing-qa.md) - Testing: cómo ejecutar tests, estructura QA, test utilities
- [25-build-system.md](25-build-system.md) - Build: Maven, perfiles, code generation, Docker

### Nivel 6 - Guías de Desarrollo
- [26-how-to-add-grpc-endpoint.md](26-how-to-add-grpc-endpoint.md) - Cómo añadir un nuevo endpoint gRPC
- [27-how-to-add-bpmn-element.md](27-how-to-add-bpmn-element.md) - Cómo añadir soporte para un nuevo elemento BPMN
- [28-how-to-add-exporter.md](28-how-to-add-exporter.md) - Cómo crear un exporter custom
- [29-how-to-modify-state.md](29-how-to-modify-state.md) - Cómo modificar el estado del engine
- [30-development-workflow.md](30-development-workflow.md) - Flujo de desarrollo, convenciones, herramientas
- [31-gateway-embedded-vs-standalone.md](31-gateway-embedded-vs-standalone.md) - Gateway embebido vs standalone: configuración, arranque, diferencias

## Estado de la Documentación

| Doc | Estado | Prioridad |
|-----|--------|-----------|
| 00-project-overview | ✅ Completado | P0 - Crítico |
| 01-architecture-overview | ✅ Completado | P0 - Crítico |
| 02-broker | ✅ Completado | P0 - Crítico |
| 03-engine | ✅ Completado | P0 - Crítico |
| 04-gateway | ✅ Completado | P0 - Crítico |
| 05-protocol | ✅ Completado | P1 - Alto |
| 06-atomix-raft | ✅ Completado | P1 - Alto |
| 07-logstreams | ✅ Completado | P1 - Alto |
| 08-stream-platform | ✅ Completado | P1 - Alto |
| 09-state-zb-db | ✅ Completado | P1 - Alto |
| 10-scheduler | ✅ Completado | P2 - Medio |
| 11-snapshots-journal | ✅ Completado | P2 - Medio |
| 12-bpmn-execution | ✅ Completado | P0 - Crítico |
| 13-dmn-feel | ✅ Completado | P2 - Medio |
| 14-exporters | ✅ Completado | P1 - Alto |
| 15-jobs-workers | ✅ Completado | P1 - Alto |
| 16-messaging | ✅ Completado | P2 - Medio |
| 17-backup-restore | ✅ Completado | P3 - Bajo |
| 18-java-client | ✅ Completado | P1 - Alto |
| 19-go-client-zbctl | ✅ Completado | P2 - Medio |
| 20-spring-boot-sdk | ✅ Completado | P2 - Medio |
| 21-topology-clustering | ✅ Completado | P2 - Medio |
| 22-transport-networking | ✅ Completado | P3 - Bajo |
| 23-auth-identity | ✅ Completado | P2 - Medio |
| 24-testing-qa | ✅ Completado | P1 - Alto |
| 25-build-system | ✅ Completado | P1 - Alto |
| 26-how-to-add-grpc-endpoint | ✅ Completado | P1 - Alto |
| 27-how-to-add-bpmn-element | ✅ Completado | P2 - Medio |
| 28-how-to-add-exporter | ✅ Completado | P2 - Medio |
| 29-how-to-modify-state | ✅ Completado | P2 - Medio |
| 30-development-workflow | ✅ Completado | P1 - Alto |

## Orden de Trabajo Recomendado

### Fase 1 - Fundamentos (empezar aquí)
1. `00-project-overview` → Tener claro qué es y cómo compilar
2. `01-architecture-overview` → Entender el diagrama general
3. `25-build-system` → Poder compilar y trabajar con el proyecto
4. `30-development-workflow` → Saber cómo desarrollar

### Fase 2 - Core Engine (entender cómo funciona)
5. `05-protocol` → Entender el formato de datos
6. `08-stream-platform` → Entender el patrón command/event
7. `09-state-zb-db` → Entender el almacenamiento de estado
8. `03-engine` → El motor de ejecución
9. `02-broker` → El servidor

### Fase 3 - APIs y Clientes (cómo se usa)
10. `04-gateway` → API gRPC/REST
11. `18-java-client` → Cliente principal
12. `15-jobs-workers` → Sistema de jobs
13. `12-bpmn-execution` → Ejecución BPMN
14. `24-testing-qa` → Cómo probar cambios

### Fase 4 - Infraestructura Distribuida
15. `06-atomix-raft` → Consenso
16. `07-logstreams` → Logs
17. `10-scheduler` → Planificador
18. `11-snapshots-journal` → Persistencia
19. `21-topology-clustering` → Topología

### Fase 5 - Funcionalidades Secundarias
20. `14-exporters` → Exportaciones
21. `13-dmn-feel` → Decisiones
22. `16-messaging` → Mensajes
23. `23-auth-identity` → Autenticación
24. Resto de documentos

### Fase 6 - Guías How-To
25-28. Guías prácticas paso a paso
