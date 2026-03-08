# Análisis de Actualización de Dependencias - OpenCamunda

**Fecha:** Marzo 2026
**Versión actual del proyecto:** 8.5.25
**Java:** 21

---

## Resumen Ejecutivo

El proyecto está basado en **Spring Boot 3.4.10** con **Spring Framework 6.2.11**.
La última versión estable de Spring Boot es **4.0.3** (basada en Spring Framework 7.0.5).
La migración a Spring Boot 4.0 es el cambio más significativo e impactante, ya que arrastra una
reestructuración modular completa del ecosistema Spring y el salto a Jakarta EE 11 (Servlet 6.1).

---

## 1. Spring Boot / Spring Framework / Spring Security

### Estado actual vs última versión

| Dependencia | Versión actual | Última estable | Salto |
|---|---|---|---|
| Spring Boot | 3.4.10 | **4.0.3** | **MAJOR** |
| Spring Framework | 6.2.11 | **7.0.5** | **MAJOR** |
| Spring Security | 6.5.5 | **7.0.3** | **MAJOR** |

### Ruta de migración recomendada

1. **Paso intermedio:** Subir a Spring Boot **3.5.11** (última 3.x GA) como paso previo. Esto permite absorber deprecations antes del salto major.
2. **Paso final:** Subir a Spring Boot **4.0.3**.

### Impacto en el proyecto (Spring Boot 3.4 → 4.0)

**Complejidad estimada: ALTA**

#### Cambios críticos:

1. **Modularización completa de Spring Boot 4.0:**
   - Spring Boot 4.0 tiene un diseño modular nuevo. Los artefactos monolíticos (spring-boot-autoconfigure, spring-boot-actuator-autoconfigure) se dividen en módulos pequeños y específicos.
   - Todos los starters se renombraron/reorganizaron (ej: `spring-boot-starter-web` → `spring-boot-starter-webmvc`).
   - **Impacto:** El proyecto usa `spring-boot-starter-webflux`, `spring-boot-starter-actuator`, `spring-boot-starter-test`, `spring-boot-starter-json`. Todos necesitan revisión.
   - **Mitigación:** Existe `spring-boot-starter-classic` como puente temporal.

2. **Jakarta EE 11 / Servlet 6.1:**
   - Spring Boot 4.0 requiere Servlet 6.1 como baseline.
   - **Impacto:** Moderado en este proyecto (se usa WebFlux más que Servlet tradicional).

3. **Jackson 3 como librería JSON preferida:**
   - Spring Boot 4.0 adopta Jackson 3 por defecto.
   - Jackson 3 cambia group IDs y paquetes: `com.fasterxml.jackson` → `tools.jackson`.
   - `@JsonComponent` → `@JacksonComponent`, `@JsonMixin` → `@JacksonMixin`.
   - **Impacto:** ALTO. El proyecto hace uso extensivo de Jackson (~50+ usos de ObjectMapper). `SdkObjectMapper`, `JsonMapperConfiguration` y múltiples módulos usan `@JsonProperty`, `readValue()`, `writeValueAsString()`.
   - **Mitigación:** Existe compatibilidad con Jackson 2 via `spring-boot-jackson2` (deprecated).
   - Jackson actual: 2.18.4.1. Jackson 3.x.x sería necesario a medio plazo.

4. **@MockBean/@SpyBean deprecados:**
   - Se deben migrar a `@MockitoBean`/`@MockitoSpyBean` de Spring Framework.
   - **Impacto:** Moderado. Se encontraron ~5 usos de `@MockBean` en:
     - `AnnotationProcessorConfigurationTest.java`
     - `ErrorMapperTest.java`
     - `UserTaskControllerTest.java`
     - `TopologyControllerTest.java`

5. **Cambios en paquetes y auto-configuración:**
   - `BootstrapRegistry` movido a `org.springframework.boot.bootstrap`.
   - `EnvironmentPostProcessor` movido de `org.springframework.boot.env` a `org.springframework.boot`.
   - `@EntityScan` movido a `org.springframework.boot.persistence.autoconfigure`.
   - **Impacto:** Bajo en este proyecto (no usa JPA/Hibernate).

6. **Elasticsearch Client:**
   - Auto-configuración migrada de `RestClient` a `Rest5Client`.
   - `RestClientBuilderCustomizer` → `Rest5ClientBuilderCustomizer`.
   - `org.elasticsearch.client:elasticsearch-rest-client` ya no se gestiona.
   - **Impacto:** ALTO. El exporter de Elasticsearch (`zeebe-elasticsearch-exporter`) se ve directamente afectado.

7. **Spring Security 7.0:**
   - Cambios significativos en configuración de seguridad.
   - **Impacto:** Moderado. El proyecto usa `spring-security` y el módulo `zeebe-auth`.

8. **Testing:**
   - `@SpringBootTest` ya no incluye MockMVC automáticamente (requiere `@AutoConfigureMockMvc`).
   - `TestRestTemplate` necesita `@AutoConfigureTestRestTemplate` explícito.
   - **Impacto:** Bajo-Moderado. Los tests REST usan `WebTestClient` que es el approach recomendado.

---

## 2. Dependencias Principales - Análisis Individual

### 2.1 gRPC

| | |
|---|---|
| **Actual** | 1.65.1 |
| **Última estable** | ~1.72.x |
| **Complejidad** | BAJA |
| **Notas** | Actualizaciones menores retrocompatibles. El proyecto usa gRPC extensivamente para la comunicación broker-gateway. Actualizar es straightforward. |

### 2.2 Protobuf

| | |
|---|---|
| **Actual** | 3.25.8 |
| **Última estable** | ~4.31.x (renombrado editions) / 3.28.x (última 3.x) |
| **Complejidad** | MEDIA |
| **Notas** | Protobuf 4.x introduce el formato "editions" reemplazando proto2/proto3 syntax. El proyecto usa `gateway-protocol` con .proto files. Se puede quedar en 3.28.x para evitar el salto major. Si se sube a 4.x, hay que actualizar `protobuf-maven-plugin`. |

### 2.3 Jackson

| | |
|---|---|
| **Actual** | 2.18.4.1 |
| **Última estable** | 2.19.x / 3.0.x (preview) |
| **Complejidad** | MEDIA (2.19) / MUY ALTA (3.0) |
| **Notas** | Jackson 3.0 es un salto major con cambios de group IDs y paquetes. Para Spring Boot 3.x se puede actualizar a 2.19.x sin problemas. Jackson 3.0 solo es necesario si se sube a Spring Boot 4.0. Nota: `jackson-annotations` versión actual es 2.17.3, desalineada con jackson BOM 2.18.4.1 — considerar unificar. |

### 2.4 Netty

| | |
|---|---|
| **Actual** | 4.1.127.Final |
| **Última estable** | 4.1.x (última patch) / 4.2.x (preview) |
| **Complejidad** | BAJA |
| **Notas** | Ya está en una versión muy reciente. Mantener actualizado con patches menores. Se usa para el transport layer del cluster. |

### 2.5 Elasticsearch Client

| | |
|---|---|
| **Actual** | 8.9.2 |
| **Última estable** | 8.17.x |
| **Complejidad** | MEDIA-ALTA |
| **Notas** | El salto de 8.9 a 8.17 puede incluir cambios de API en el Java client. El módulo `zeebe-elasticsearch-exporter` y `zeebe-exporter-test` se ven afectados directamente. Si se migra a Spring Boot 4.0, el módulo `elasticsearch-rest-client` desaparece, forzando el uso de `Rest5Client`. |

### 2.6 OpenSearch Client

| | |
|---|---|
| **Actual** | 2.5.0 |
| **Última estable** | 2.19.x |
| **Complejidad** | MEDIA |
| **Notas** | Gran salto de versión. Afecta a `zeebe-opensearch-exporter`. Revisar changelog para breaking changes en el Java client. |

### 2.7 RocksDB JNI

| | |
|---|---|
| **Actual** | 8.11.4 |
| **Última estable** | 9.10.x / 8.11.x (última 8.x) |
| **Complejidad** | BAJA (patch) / MEDIA (9.x) |
| **Notas** | Componente crítico para el state store (zb-db). RocksDB 9.x puede incluir cambios de API en el JNI binding. Recomendable verificar retrocompatibilidad del formato de datos. Tests intensivos necesarios. |

### 2.8 Micrometer

| | |
|---|---|
| **Actual** | 1.14.11 |
| **Última estable** | 1.15.x / 2.0.x |
| **Complejidad** | BAJA (1.15) / MEDIA (2.0 con Spring Boot 4) |
| **Notas** | Micrometer 2.0 viene con Spring Boot 4.0. Para Spring Boot 3.x, mantenerse en la serie 1.x. |

### 2.9 Testcontainers

| | |
|---|---|
| **Actual** | 1.19.8 |
| **Última estable** | 1.21.x |
| **Complejidad** | BAJA |
| **Notas** | Actualización retrocompatible. Mejoras en soporte para contenedores más nuevos. |

### 2.10 JUnit 5

| | |
|---|---|
| **Actual** | 5.10.5 |
| **Última estable** | 5.12.x |
| **Complejidad** | BAJA |
| **Notas** | Actualización menor. Nuevas APIs pero todas retrocompatibles. |

### 2.11 Mockito

| | |
|---|---|
| **Actual** | 5.11.0 |
| **Última estable** | 5.17.x |
| **Complejidad** | BAJA |
| **Notas** | Actualización menor retrocompatible. |

### 2.12 SLF4J / Log4j2

| | |
|---|---|
| **SLF4J actual** | 2.0.17 |
| **Log4j2 actual** | 2.23.1 |
| **SLF4J última** | 2.0.x (actual) |
| **Log4j2 última** | 2.24.x |
| **Complejidad** | BAJA |
| **Notas** | Ya en versiones muy recientes. Solo patches menores. |

### 2.13 Guava

| | |
|---|---|
| **Actual** | 33.1.0-jre |
| **Última estable** | 33.4.x |
| **Complejidad** | BAJA |
| **Notas** | Patch update. Retrocompatible. |

### 2.14 AWS SDK

| | |
|---|---|
| **Actual** | 2.25.70 |
| **Última estable** | 2.31.x |
| **Complejidad** | BAJA |
| **Notas** | Usado por `zeebe-backup-store-s3`. Actualizaciones menores en la serie 2.x. |

### 2.15 Google Cloud SDK

| | |
|---|---|
| **Actual** | 26.34.0 |
| **Última estable** | 26.55.x |
| **Complejidad** | BAJA |
| **Notas** | Usado por `zeebe-backup-store-gcs`. Actualizaciones retrocompatibles. |

### 2.16 Reactor Core / Reactor Netty

| | |
|---|---|
| **Actual** | reactor-core 3.7.11 / reactor-netty 1.2.10 |
| **Última estable** | reactor-core 3.7.x / reactor-netty 1.2.x |
| **Complejidad** | BAJA |
| **Notas** | Ya muy actualizados. Con Spring Boot 4.0 se subiría a la siguiente generación. |

### 2.17 Scala / FEEL Engine / DMN Engine

| | |
|---|---|
| **Scala actual** | 2.13.18 |
| **FEEL Engine actual** | 1.21.0 |
| **DMN Engine actual** | 1.12.0 |
| **Complejidad** | BAJA |
| **Notas** | Componentes funcionales estables. Scala 2.13 es la última de la serie 2.x (Scala 3.x requeriría reescritura de dependencias Scala). FEEL/DMN engines se mantienen estables. |

### 2.18 Agrona

| | |
|---|---|
| **Actual** | 1.20.0 |
| **Última estable** | 1.23.x |
| **Complejidad** | BAJA |
| **Notas** | Librería de bajo nivel para buffers. Retrocompatible. |

### 2.19 Bouncycastle

| | |
|---|---|
| **Actual** | 1.78.1 |
| **Última estable** | 1.80.x |
| **Complejidad** | BAJA |
| **Notas** | Librería de criptografía. Patches de seguridad. Recomendable mantener actualizada. |

### 2.20 Maven Plugins

| Plugin | Actual | Última | Notas |
|---|---|---|---|
| maven-compiler | 3.12.1 | 3.14.x | Retrocompatible |
| maven-surefire | 3.2.5 | 3.5.x | Retrocompatible |
| maven-failsafe | 3.2.5 | 3.5.x | Retrocompatible |
| spotless | 2.43.0 | 2.44.x | Retrocompatible |
| JaCoCo | 0.8.13 | 0.8.13 | Ya actualizado |

---

## 3. Mapa de Riesgos

```
                     Impacto Alto
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    │  Elasticsearch     │  Spring Boot 4.0   │
    │  Client 8.17       │  + Spring FW 7.0   │
    │                    │  + Jackson 3        │
    │                    │  + Spring Sec 7.0   │
    │                    │                    │
    ├────────────────────┼────────────────────┤
    │                    │                    │
    │  Protobuf 4.x      │  OpenSearch 2.19   │
    │  RocksDB 9.x       │                    │
    │                    │                    │
    │                    │                    │
    └────────────────────┼────────────────────┘
                         │
    Probabilidad Baja    │    Probabilidad Alta
    de problemas         │    de problemas
                         │
                     Impacto Bajo
```

---

## 4. Plan de Migración Recomendado

### Fase 1 — Actualizaciones seguras (Complejidad BAJA)

Dependencias que se pueden actualizar con confianza sin romper nada:

| Dependencia | De | A |
|---|---|---|
| Guava | 33.1.0-jre | 33.4.x |
| Testcontainers | 1.19.8 | 1.21.x |
| JUnit 5 | 5.10.5 | 5.12.x |
| Mockito | 5.11.0 | 5.17.x |
| Agrona | 1.20.0 | 1.23.x |
| Bouncycastle | 1.78.1 | 1.80.x |
| Log4j2 | 2.23.1 | 2.24.x |
| AWS SDK | 2.25.70 | 2.31.x |
| Google Cloud | 26.34.0 | 26.55.x |
| AssertJ | 3.25.3 | 3.27.x |
| gRPC | 1.65.1 | 1.72.x |
| commons-lang3 | 3.18.0 | 3.18.x (actual) |
| Maven plugins | varios | últimas menores |
| Jackson annotations | 2.17.3 | 2.18.x (alinear con BOM) |

**Esfuerzo estimado:** Build + test cycle. Bajo riesgo.

### Fase 2 — Actualizaciones moderadas

| Dependencia | De | A | Notas |
|---|---|---|---|
| Elasticsearch | 8.9.2 | 8.17.x | Requiere adaptar exporter |
| OpenSearch | 2.5.0 | 2.19.x | Requiere adaptar exporter |
| Protobuf | 3.25.8 | 3.28.x | Última 3.x, sin romper proto syntax |
| Jackson BOM | 2.18.4.1 | 2.19.x | Verificar serialización/deserialización |

**Esfuerzo estimado:** Pruebas focalizadas en exporters y serialización.

### Fase 3 — Spring Boot 3.4 → 3.5 (paso intermedio)

- Subir a Spring Boot **3.5.11** (última 3.x GA).
- Resolver todas las deprecations que emerjan.
- Subir Spring Framework a **6.2.16** y Spring Security a **6.5.8**.
- Esto permite detectar qué APIs deprecated se usan antes de que desaparezcan en 4.0.

**Esfuerzo estimado:** Moderado. Requiere compilar, testear, y resolver warnings.

### Fase 4 — Spring Boot 4.0 (salto major)

**Esta es la fase más costosa y arriesgada.**

1. Subir Spring Boot a **4.0.3** (implica Spring Framework 7.0.5 + Spring Security 7.0.3).
2. Usar `spring-boot-starter-classic` como puente temporal.
3. Migrar Jackson a 3.x o usar `spring-boot-jackson2` como bridge temporal.
4. Adaptar módulos REST/WebFlux a nuevos starters.
5. Migrar `@MockBean` → `@MockitoBean` en tests.
6. Adaptar el exporter de Elasticsearch al `Rest5Client`.
7. Adaptar cualquier cambio de paquetes de auto-configuración.
8. Tests extensivos de integración.

**Esfuerzo estimado:** Alto. Es el trabajo más grande de la actualización.

---

## 5. Resumen de Esfuerzo

| Fase | Riesgo | Esfuerzo |
|---|---|---|
| Fase 1 — Updates seguros | Bajo | 1-2 días |
| Fase 2 — Updates moderados | Medio | 3-5 días |
| Fase 3 — Spring Boot 3.5 | Medio | 2-3 días |
| Fase 4 — Spring Boot 4.0 | Alto | 2-3 semanas |

### Decisión clave: ¿Merece la pena subir a Spring Boot 4.0 ahora?

**Argumentos a favor:**
- Spring Boot 3.4 deja de tener soporte en pocos meses. 3.5 tiene soporte extendido pero también caduca.
- Jackson 3, Jakarta EE 11, y la modularización son el futuro del ecosistema.
- Cuanto más se espere, más divergencia habrá con el upstream.

**Argumentos en contra:**
- El salto es grande y costoso, especialmente por Jackson 3 y la reestructuración modular.
- El proyecto funciona correctamente con Spring Boot 3.4.
- Se puede quedar en Spring Boot 3.5.x como posición estable a medio plazo.

**Recomendación:** Ejecutar Fases 1 y 2 de inmediato (bajo riesgo, alto valor). Ejecutar Fase 3 (subir a 3.5.x) a corto plazo. Planificar Fase 4 (Spring Boot 4.0) como un proyecto dedicado.

---

## 6. Versiones Desalineadas Detectadas

| Aspecto | Detalle |
|---|---|
| `jackson-annotations` | Definida como 2.17.3, pero el Jackson BOM es 2.18.4.1. Debería heredar del BOM. |
| `version.reactor-core` / `version.reactor-netty` | Definidas manualmente, podrían heredar de Spring Boot BOM en vez de fijarse independientemente. |
| `version.byte-buddy` | 1.14.19. Mockito 5.11 trae 1.14.x también. Verificar que no haya conflictos al subir Mockito. |
