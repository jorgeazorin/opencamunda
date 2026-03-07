# 25 - Sistema de Build

## Maven Multi-Module

El proyecto usa Maven con una estructura multi-módulo de ~50 módulos. La jerarquía es:

```
pom.xml (raíz - aggregator)
├── parent/pom.xml        ← Configuración heredada por todos los módulos
├── bom/pom.xml            ← Bill of Materials (versiones centralizadas)
├── build-tools/           ← Herramientas custom de build
└── zeebe/*/pom.xml        ← Módulos individuales
```

## Compilar el Proyecto

### Build rápido (desarrollo diario)
```bash
mvn clean install -Dquickly
```
Omite: tests, checkstyle, revapi, license checks, flatten, assembly.

### Build completo sin tests
```bash
mvn clean install -DskipTests
```

### Build con tests unitarios
```bash
mvn verify -DskipITs
```

### Build con tests de integración
```bash
mvn verify
```

### Solo un módulo y sus dependencias
```bash
mvn clean install -pl zeebe/engine -am -DskipTests
```
- `-pl zeebe/engine` → solo ese módulo
- `-am` → also-make (compila sus dependencias)

### Build incremental (si ya compilaste antes)
```bash
mvn -b incremental install -DskipTests
```
Usa la extensión `incremental-module-builder` configurada en `.mvn/extensions.xml`.

## Propiedades de Skip

| Propiedad | Qué omite |
|-----------|-----------|
| `-Dquickly` | Tests + todos los checks + assembly |
| `-DskipTests` | Todos los tests |
| `-DskipUTs` | Solo tests unitarios |
| `-DskipITs` | Solo tests de integración |
| `-DskipChecks` | Checkstyle, revapi, license, enforcer |
| `-Dcheckstyle.skip=true` | Solo checkstyle |
| `-Drevapi.skip=true` | Solo API compat check |
| `-Dspotless.apply.skip=true` | Solo formateo de código |
| `-Djacoco.skip=true` | Solo cobertura |

## Perfiles Maven

### Perfiles de Testing
| Perfil | Uso | Comando |
|--------|-----|---------|
| `skip-random-tests` | Excluye tests aleatorios (Raft/Property) | `-P skip-random-tests` |
| `include-random-tests` | Solo tests aleatorios | `-P include-random-tests` |
| `include-strace-tests` | Solo tests de strace | `-P include-strace-tests` |
| `include-performance-tests` | Solo tests de rendimiento | `-P include-performance-tests` |
| `parallel-tests` | Ejecución paralela (forkCount=0.5C, 2 threads JUnit) | `-P parallel-tests` |
| `extract-flaky-tests` | Extrae tests flaky para CI | `-P extract-flaky-tests` |

### Perfiles de Calidad
| Perfil | Uso | Comando |
|--------|-----|---------|
| `spotbugs` | Análisis estático de bugs (effort=Max, threshold=Low) | `-P spotbugs` |
| `prepare-offline` | Descarga deps para build offline | `-P prepare-offline` |

## Code Generation (3 pipelines)

### 1. SBE (Simple Binary Encoding) - Protocolo binario
**Módulo**: `zeebe/protocol/`
**Plugin**: `exec-maven-plugin` ejecutando `SbeTool`
**Fase**: `generate-sources`

```
Input:  src/main/resources/protocol.xml
        src/main/resources/cluster-management-protocol.xml
Output: target/generated-sources/sbe/
```

Genera los enums y records del protocolo: `RecordType`, `ValueType`, `Intent`, etc.

**Flags SBE**:
- `sbe.java.generate.interfaces=true` → genera interfaces
- `sbe.decode.unknown.enum.values=true` → compatibilidad forward
- `sbe.xinclude.aware=true` → permite incluir XMLs

**IMPORTANTE**: Thread-unsafe en builds paralelos. Usa `exec:exec` con proceso separado.

### 2. Protobuf/gRPC - API del Gateway
**Módulo**: `zeebe/gateway-protocol-impl/`
**Plugin**: `protobuf-maven-plugin` (0.6.1)
**Fase**: `generate-sources`

```
Input:  zeebe/gateway-protocol/src/main/proto/gateway.proto
Output: target/generated-sources/protobuf/
```

Genera:
- Stubs gRPC del servicio `Gateway`
- Clases Java para todos los messages (requests/responses)

**Protoc**: 3.25.8
**gRPC Plugin**: protoc-gen-grpc-java 1.65.1

También existe un perfil de golang que genera código Go para el cliente.

### 3. OpenAPI - REST API
**Módulo**: `zeebe/gateway-rest/`
**Plugin**: `openapi-generator-maven-plugin` (7.4.0)
**Fase**: `generate-sources`

```
Input:  zeebe/gateway-protocol/src/main/proto/rest-api.yaml
Output: Modelos Java en io.camunda.zeebe.gateway.protocol.rest
```

Genera solo modelos (no controladores). Usa Jackson, Spring Boot 3, sin nullable OpenAPI.

## Quality Checks

### Spotless (Formateo de Código)
- **Formato**: Google Java Format 1.21.0 estilo GOOGLE
- **Markdown**: Flexmark
- **Ejecutar**: `mvn spotless:apply` (auto-formatea)
- **Verificar**: `mvn spotless:check`

### Checkstyle (Estilo de Código)
- **Versión**: 10.14.2
- **Config**: `build-tools/src/main/resources/check/.checkstyle.xml`
- **Falla el build**: Sí, en cualquier violación
- **Header**: Requiere header de licencia en cada archivo

### SpotBugs (Detección de Bugs)
- **Versión**: 4.8.6.7
- **Activar**: `-P spotbugs`
- **Filtros**: `build-tools/src/main/resources/spotbugs/`
- **Esfuerzo**: Máximo (más lento, más exhaustivo)

### RevAPI (Compatibilidad de API)
- **Versión**: 0.15.1
- **Compara contra**: versión 8.5.24
- **Falla si**: hay breaking changes binarios o de fuente
- **Config**: `build-tools/src/main/resources/revapi/`
- **Ignorar cambios**: `revapi/ignored-changes.json`

### Enforcer (Reglas Maven)
- `dependencyConvergence` → todas las versiones deben coincidir
- `banDuplicatePomDependencyVersions` → no duplicar versiones

## Testing

### Surefire (Tests Unitarios)
- **Versión**: 3.2.5
- Excluye grupos: `performance`, `strace`
- `redirectTestOutputToFile=true` → output a fichero
- `trimStackTrace=false` → stacktraces completos
- Custom listeners: `ZeebeTestListener`, `ZeebeConsoleOutputReporter`

### Failsafe (Tests de Integración)
- **Versión**: 3.2.5
- `rerunFailingTestsCount=3` → reintenta tests fallidos
- Custom reporters igual que surefire
- Patrón de descubrimiento: `*IT*.java`, `*Test*.java`

### Ejecución Paralela (perfil `parallel-tests`)
```properties
forkCount=0.5C              # Mitad de CPUs
junit.jupiter.execution.parallel.enabled=true
junit.jupiter.execution.parallel.config.strategy=fixed
junit.jupiter.execution.parallel.config.fixed.parallelism=2
```

## Docker Build

### Multi-stage (requiere BuildKit)
```bash
DOCKER_BUILDKIT=1 docker build -t opencamunda .
```

### Stages:
1. **base**: Ubuntu Noble + tini + locales
2. **jre-build**: JDK 21 Temurin → custom JRE vía jlink (comprimido, sin debug)
3. **java**: base + custom JRE + Class Data Sharing
4. **build**: Compila desde fuente con cache de Maven
5. **distball**: Extrae tarball pre-compilado (alternativa)
6. **app**: Imagen final, usuario no-root `camunda` (UID 1001)

### Comando de build Maven en Docker:
```bash
./mvnw -B -am -pl dist package -T1C -DskipChecks -DskipTests -Dmaven.gitcommitid.skip=true
```

### Puertos expuestos:
- `8080` → REST API / actuator
- `26500-26502` → gRPC, cluster command, cluster internal

### Volúmenes:
- `/usr/local/zeebe/data` → datos persistentes
- `/usr/local/zeebe/logs` → logs

## Extensiones Maven (`.mvn/extensions.xml`)

| Extensión | Uso |
|-----------|-----|
| `incremental-module-builder` 0.2.0 | Build incremental: `mvn -b incremental install` |
| `maven-profiler` 3.2 | Profiling del tiempo de build |
| `os-maven-plugin` 1.7.1 | Detección de plataforma (para protoc nativo) |

## JVM Config (`.mvn/jvm.config`)
Abre módulos internos del JDK necesarios para el compilador y Google Java Format:
```
--add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED
--add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED
--add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED
--add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.text=ALL-UNNAMED
--add-opens=java.desktop/java.awt.font=ALL-UNNAMED
```

## Commit Conventions

**Formato**: `type(scope): description`

**Tipos permitidos**: `build`, `ci`, `deps`, `docs`, `feat`, `fix`, `merge`, `perf`, `refactor`, `revert`, `style`, `test`

**Reglas**:
- Tipo en minúsculas, obligatorio
- Scope obligatorio
- Header máx 120 chars
- Body máx 120 chars por línea

## Versiones Clave de Dependencias

| Dependencia | Versión | Uso |
|------------|---------|-----|
| Java | 21 | Runtime (8 para clientes) |
| Spring Boot | 3.4.10 | Framework |
| gRPC | 1.65.1 | API gateway |
| Protobuf | 3.25.8 | Serialización gRPC |
| RocksDB | 8.11.4 | Estado persistente |
| SBE | 1.30.0 | Protocolo binario |
| Netty | 4.1.127 | Transporte red |
| Jackson | 2.18.4 | JSON |
| Scala | 2.13.17 | FEEL/DMN engines |
| Elasticsearch | 8.9.2 | Exporter |
| JUnit | 5.10.5 | Testing |
| Mockito | 5.11.0 | Mocking |
| AssertJ | 3.25.3 | Assertions |
| Testcontainers | 1.19.8 | Integration tests |
