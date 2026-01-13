{{- define "wallaby.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "wallaby.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "wallaby.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "wallaby.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "wallaby.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "wallaby.otelEnv" -}}
{{- with .Values.observability.metrics }}
{{- if and .enabled .otel.endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .otel.endpoint | quote }}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: {{ .otel.insecure | toString | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ .otel.protocol | default "grpc" | quote }}
- name: OTEL_METRICS_EXPORTER
  value: "otlp"
{{- if .otel.interval }}
- name: WALLABY_OTEL_METRICS_INTERVAL
  value: {{ .otel.interval | quote }}
{{- end }}
{{- if .otel.serviceName }}
- name: OTEL_SERVICE_NAME
  value: {{ .otel.serviceName | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- with .Values.observability.traces }}
{{- if and .enabled .otel.endpoint }}
- name: OTEL_TRACES_EXPORTER
  value: "otlp"
{{- end }}
{{- end }}
{{- with .Values.observability.profiling }}
{{- if .enabled }}
- name: WALLABY_PPROF_ENABLED
  value: "true"
- name: WALLABY_PPROF_LISTEN
  value: {{ printf ":%d" (int .port) | quote }}
{{- end }}
{{- end }}
{{- end -}}
