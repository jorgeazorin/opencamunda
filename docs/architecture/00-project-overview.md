# 00 - Project Overview

## ¿Qué es este proyecto?

**OpenCamunda** es un fork de **Camunda Zeebe v8.5.25**, la última versión con licencia comunitaria libre antes de que Camunda cambiara a una licencia solo para uso no productivo.

Zeebe es un **motor de orquestación de workflows** distribuido, diseñado para ejecutar procesos BPMN 2.0 a gran escala. Funciona como un sistema de **event sourcing** donde todos los cambios de estado se almacenan como un log de eventos inmutable.

## Conceptos Clave

### ¿Qué hace Zeebe?
1. **Despliega** definiciones de procesos BPMN 2.0  
2. **Crea instancias** de esos procesos
3. **Ejecuta** los elementos BPMN (service tasks, gateways, events, etc.)
4. **Distribuye jobs** a workers externos que ejecutan la lógica de negocio
5. **Gestiona el estado** de todas las instancias de proceso de forma persistente y distribuida

### Modelo de Ejecución
```
Cliente → Gateway (gRPC/REST) → Broker → Engine → Log de Eventos
                                                        ↓
                                               State (RocksDB)
                                                        ↓
                                               Exporters → Elasticsearch
```

- Los **clientes** envían comandos (deploy, create instance, complete job)
- El **gateway** traduce las peticiones gRPC/REST a comandos internos
- El **broker** gestiona particiones y consenso (Raft)
- El **engine** procesa los comandos y genera eventos
- Los **eventos** se persisten en un log append-only
- El **estado** se mantiene en RocksDB para consultas rápidas
- Los **exporters** envían datos a sistemas externos (Elasticsearch/OpenSearch)

### Particionamiento
- Los datos se dividen en **particiones** (como Kafka)
- Cada partición tiene un **líder** (escribe) y **seguidores** (replican)
- El consenso usa **Raft** (implementación Atomix)
- Las instancias de proceso se asignan a particiones por su key

## Estructura del Repositorio

```
opencamunda/
├── pom.xml                              # POM raíz, define todos los módulos
├── parent/pom.xml                       # Configuración padre (plugins, checks)
├── bom/pom.xml                          # Bill of Materials (versiones de deps)
├── build-tools/                         # Herramientas de build custom
├── spring-boot-starter-camunda-sdk/     # Spring Boot starter
├── clients/go/                          # Cliente Go + zbctl CLI
├── docs/                                # Documentación
├── monitor/                             # Grafana + Prometheus dashboards
├── zeebe/                               # ← TODO EL CÓDIGO CORE ESTÁ AQUÍ
│   ├── broker/                          # Servidor Zeebe
│   ├── engine/                          # Motor de ejecución BPMN
│   ├── gateway/                         # Gateway gRPC
│   ├── gateway-rest/                    # Gateway REST
│   ├── gateway-protocol/                # Definiciones .proto
│   ├── gateway-protocol-impl/           # Código generado gRPC
│   ├── protocol/                        # Protocolo binario SBE
│   ├── protocol-impl/                   # Implementación del protocolo
│   ├── atomix/                          # Raft + clustering (fork de Atomix)
│   ├── logstreams/                      # Log append-only
│   ├── stream-platform/                 # Stream processing framework
│   ├── zb-db/                           # Abstracción sobre RocksDB
│   ├── scheduler/                       # Actor scheduler
│   ├── snapshot/                        # Gestión de snapshots
│   ├── journal/                         # Write-ahead logging
│   ├── topology/                        # Topología del cluster
│   ├── transport/                       # Transporte de red
│   ├── broker-client/                   # Cliente para comunicación entre brokers
│   ├── auth/                            # Autenticación
│   ├── clients/java/                    # Cliente Java
│   ├── clients/zeebe-client-spring/     # Spring integration
│   ├── bpmn-model/                      # Parser BPMN 2.0
│   ├── dmn/                             # Motor DMN (decisiones)
│   ├── feel/                            # FEEL expression language
│   ├── expression-language/             # Motor de expresiones
│   ├── exporters/                       # Elasticsearch + OpenSearch exporters
│   ├── exporter-api/                    # API para exporters custom
│   ├── backup/                          # Sistema de backup
│   ├── backup-stores/                   # Implementaciones S3, GCS, Azure
│   ├── restore/                         # Restauración de backups
│   ├── util/                            # Utilidades comunes
│   ├── msgpack-core/                    # MessagePack serialización
│   ├── msgpack-value/                   # MessagePack values
│   ├── test-util/                       # Utilidades de testing
│   ├── qa/                              # Integration tests
│   └── benchmarks/                      # Benchmarks de rendimiento
```

## Identidad del Proyecto

| Propiedad | Valor |
|-----------|-------|
| GroupId | `io.camunda` |
| Versión | `8.5.25` |
| Java mínimo | JDK 21 (core), JDK 8 (clientes) |
| Licencia | Zeebe Community License 1.1 + Apache 2.0 (clientes/APIs) |
| Build | Maven 3.x |

## Cómo Compilar

### Build rápido (sin tests ni checks)
```bash
mvn clean install -Dquickly
```

### Build completo (sin tests)
```bash
mvn clean install -DskipTests
```

### Build completo con tests
```bash
mvn verify
```

### Solo un módulo específico
```bash
mvn clean install -pl zeebe/engine -am -DskipTests
```
- `-pl zeebe/engine` → solo el módulo engine
- `-am` → also-make (compila dependencias necesarias)

## Cómo Ejecutar

### Docker
```bash
DOCKER_BUILDKIT=1 docker build -t opencamunda .
docker run -p 26500:26500 -p 8080:8080 opencamunda
```

### Puertos
| Puerto | Protocolo | Uso |
|--------|-----------|-----|
| 26500 | gRPC | API principal (clientes se conectan aquí) |
| 26501 | Internal | Comunicación entre brokers (command API) |
| 26502 | Internal | Comunicación interna del cluster |
| 8080 | HTTP | REST API + actuator/management |

## Tecnologías Principales

| Tecnología | Versión | Uso |
|------------|---------|-----|
| Java | 21 | Runtime principal |
| Spring Boot | 3.4.10 | Framework para configuración y REST |
| gRPC | 1.65.1 | API principal de comunicación |
| Protobuf | 3.25.8 | Serialización gRPC |
| RocksDB | 8.11.4 | Almacenamiento de estado |
| Netty | 4.1.127 | Transporte de red |
| SBE | 1.30.0 | Serialización binaria del protocolo |
| Scala | 2.13.17 | FEEL/DMN engines |
| Jackson | 2.18.4 | JSON serialización |
| Elasticsearch | 8.9.2 | Exporter de datos |
| Log4j | 2.23.1 | Logging |

## Siguiente Documento

→ [01-architecture-overview.md](01-architecture-overview.md) - Arquitectura detallada del sistema
