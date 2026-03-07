{{/*
Common labels
*/}}
{{- define "opencamunda.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Zeebe broker labels
*/}}
{{- define "opencamunda.zeebe.labels" -}}
{{ include "opencamunda.labels" . }}
app.kubernetes.io/component: zeebe-broker
{{- end }}

{{/*
Zeebe gateway labels
*/}}
{{- define "opencamunda.gateway.labels" -}}
{{ include "opencamunda.labels" . }}
app.kubernetes.io/component: zeebe-gateway
{{- end }}

{{/*
Elasticsearch labels
*/}}
{{- define "opencamunda.elasticsearch.labels" -}}
{{ include "opencamunda.labels" . }}
app.kubernetes.io/component: elasticsearch
{{- end }}

{{/*
Zeebe image
*/}}
{{- define "opencamunda.zeebe.image" -}}
{{- if .Values.zeebe.image.registry -}}
{{ .Values.zeebe.image.registry }}/{{ .Values.zeebe.image.repository }}:{{ .Values.zeebe.image.tag | default .Chart.AppVersion }}
{{- else -}}
{{ .Values.zeebe.image.repository }}:{{ .Values.zeebe.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end }}

{{/*
Gateway image
*/}}
{{- define "opencamunda.gateway.image" -}}
{{- if .Values.zeebeGateway.image.registry -}}
{{ .Values.zeebeGateway.image.registry }}/{{ .Values.zeebeGateway.image.repository }}:{{ .Values.zeebeGateway.image.tag | default .Chart.AppVersion }}
{{- else -}}
{{ .Values.zeebeGateway.image.repository }}:{{ .Values.zeebeGateway.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end }}

{{/*
Zeebe cluster name
*/}}
{{- define "opencamunda.zeebe.clusterName" -}}
{{ .Release.Name }}-zeebe
{{- end }}

{{/*
Elasticsearch URL
*/}}
{{- define "opencamunda.elasticsearch.url" -}}
{{- if and .Values.exporter.elasticsearch.url (ne .Values.exporter.elasticsearch.url "") -}}
{{ .Values.exporter.elasticsearch.url }}
{{- else -}}
http://{{ .Release.Name }}-elasticsearch:9200
{{- end -}}
{{- end }}
