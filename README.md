# OpenCamunda — Fork libre de Zeebe 8.5

Fork comunitario del motor de workflow [Zeebe](https://github.com/camunda/zeebe) (Camunda Platform 8), basado en la **última versión antes del cambio de licencia a uso no productivo** (8.5.25).

## ¿Por qué este fork?

A partir de la versión 8.6, Camunda cambió la licencia del proyecto a una que **solo permite uso no productivo** sin pagar una licencia comercial. Este fork parte de la versión **8.5.25**, la última que mantiene la [Zeebe Community License v1.1](/licenses/ZEEBE-COMMUNITY-LICENSE-1.1.txt), que permite:

- Uso **gratuito en producción** dentro de una empresa
- Modificación, distribución y sublicenciamiento del código
- La única restricción es que no se puede ofrecer como **servicio BPaaS a terceros** (Commercial Process Automation Service)

El objetivo de este fork es mantener una base de Zeebe que pueda seguir evolucionando libremente para uso empresarial interno sin depender de la licencia comercial de Camunda.

## ¿Qué se ha hecho?

Se ha limpiado el repositorio original eliminando todo lo que pertenecía a productos comerciales de Camunda que no forman parte del motor Zeebe:

- **Eliminados**: `operate/`, `optimize/`, `identity/` (stubs vacíos de productos comerciales)
- **Eliminado**: `license/` (cabecera de licencia "non-production only" de Camunda 8.6+)
- **Eliminados**: workflows de CI/CD, issue templates y GitHub Actions de Operate e Identity
- **Eliminados**: dashboards de Grafana y configuración de Prometheus de Operate y Tasklist
- **Limpiados**: referencias a repositorios Maven de Identity y exclusiones de Operate en los POMs

Todo el código funcional de Zeebe se conserva intacto.

## ¿Qué contiene?

| Módulo | Descripción | Licencia |
|---|---|---|
| `zeebe/broker/` | Broker principal de Zeebe | ZCL 1.1 |
| `zeebe/gateway/` | Gateway gRPC | ZCL 1.1 |
| `zeebe/gateway-rest/` | Gateway REST | ZCL 1.1 |
| `zeebe/engine/` | Motor de ejecución de procesos | ZCL 1.1 |
| `zeebe/atomix/` | Capa de clustering (Raft) | Apache 2.0 |
| `zeebe/bpmn-model/` | API de modelo BPMN | Apache 2.0 |
| `zeebe/clients/java/` | Cliente Java | Apache 2.0 |
| `zeebe/clients/zeebe-client-spring/` | Integración Spring Boot | Apache 2.0 |
| `clients/go/` | Cliente Go | Apache 2.0 |
| `zeebe/exporter-api/` | API de exportadores | Apache 2.0 |
| `zeebe/exporters/` | Exportadores (Elasticsearch, OpenSearch) | ZCL 1.1 |
| `zeebe/protocol/` | Definiciones del protocolo | Apache 2.0 |
| `zeebe/dmn/` | Motor de decisiones DMN | ZCL 1.1 |
| `zeebe/feel/` | Evaluador de expresiones FEEL | ZCL 1.1 |
| `spring-boot-starter-camunda-sdk/` | Spring Boot Starter | Apache 2.0 |
| `dist/` | Distribución empaquetada | ZCL 1.1 |

## Características de Zeebe

* Diseña procesos visualmente en [BPMN 2.0](https://www.omg.org/spec/BPMN/2.0.2/)
* Elige tu lenguaje de programación (Java, Go, o cualquiera vía gRPC/REST)
* Despliega con [Docker](https://www.docker.com/) y [Kubernetes](https://kubernetes.io/)
* Construye procesos que reaccionan a mensajes de [Kafka](https://kafka.apache.org/) y otras colas
* Escalado horizontal para alto rendimiento
* Tolerancia a fallos (sin necesidad de base de datos relacional)
* Exporta datos de procesos para monitorización y análisis

## Documentación útil

* [Conceptos técnicos](https://docs.camunda.io/docs/components/zeebe/technical-concepts/)
* [Procesos BPMN](https://docs.camunda.io/docs/components/modeler/bpmn/bpmn-primer/)
* [Instalación y configuración](https://docs.camunda.io/docs/self-managed/zeebe-deployment/)
* [Cliente Java](https://docs.camunda.io/docs/apis-clients/java-client/)
* [Cliente Go](https://docs.camunda.io/docs/apis-clients/go-client/)
* [Construcción de imágenes Docker](/zeebe/docs/building_docker_images.md)

## Construir desde fuente

```bash
./mvnw clean install -DskipTests
```

## Contribuir

Lee la [guía de contribución](/CONTRIBUTING.md).

## Licencia

Los archivos fuente de Zeebe están disponibles bajo la [Zeebe Community License
Version 1.1](/licenses/ZEEBE-COMMUNITY-LICENSE-1.1.txt) excepto las partes listadas
a continuación, que están bajo [Apache License, Version 2.0](/licenses/APACHE-2.0.txt).
Consulta los archivos fuente individuales para más detalle.

Disponible bajo [Apache License, Version 2.0](/licenses/APACHE-2.0.txt):
- Cliente Java ([clients/java](/zeebe/clients/java))
- Cliente Go ([clients/go](/clients/go))
- Exporter API ([exporter-api](/zeebe/exporter-api))
- Protocol ([protocol](/zeebe/protocol))
- Gateway Protocol Implementation ([gateway-protocol-impl](/zeebe/gateway-protocol-impl))
- BPMN Model API ([bpmn-model](/zeebe/bpmn-model))
- Atomix ([atomix](/zeebe/atomix))
- Journal ([journal](/zeebe/journal))
- Spring Boot Starter ([spring-boot-starter-camunda-sdk](/spring-boot-starter-camunda-sdk))
- Benchmarks ([benchmarks](/zeebe/benchmarks/project))

### Clarification on gRPC Code Generation

The Zeebe Gateway Protocol (API) as published in the
[gateway-protocol](/gateway-protocol/src/main/proto/gateway.proto) is licensed
under the [Zeebe Community License 1.1](/licenses/ZEEBE-COMMUNITY-LICENSE-1.1.txt). Using gRPC tooling to generate stubs for
the protocol does not constitute creating a derivative work under the Zeebe Community License 1.1 and no licensing restrictions are imposed on the
resulting stub code by the Zeebe Community License 1.1.
