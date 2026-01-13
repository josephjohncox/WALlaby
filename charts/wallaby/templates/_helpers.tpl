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
{{- $metrics := .Values.observability.metrics -}}
{{- $traces := .Values.observability.traces -}}
{{- $endpoint := "" -}}
{{- $insecure := "" -}}
{{- $protocol := "" -}}
{{- if and $metrics.enabled $metrics.otel.endpoint -}}
{{- $endpoint = $metrics.otel.endpoint -}}
{{- $insecure = $metrics.otel.insecure -}}
{{- $protocol = $metrics.otel.protocol -}}
{{- else if and $traces.enabled $traces.otel.endpoint -}}
{{- $endpoint = $traces.otel.endpoint -}}
{{- $insecure = $traces.otel.insecure -}}
{{- $protocol = $traces.otel.protocol -}}
{{- end -}}
{{- if $endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $endpoint | quote }}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: {{ $insecure | toString | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ $protocol | default "grpc" | quote }}
{{- end }}
{{- if and $metrics.enabled $endpoint }}
- name: OTEL_METRICS_EXPORTER
  value: "otlp"
{{- if $metrics.otel.interval }}
- name: WALLABY_OTEL_METRICS_INTERVAL
  value: {{ $metrics.otel.interval | quote }}
{{- end }}
{{- end }}
{{- if and $traces.enabled $endpoint }}
- name: OTEL_TRACES_EXPORTER
  value: "otlp"
{{- end }}
{{- $serviceName := "" -}}
{{- if $metrics.otel.serviceName }}
{{- $serviceName = $metrics.otel.serviceName -}}
{{- else if $traces.otel.serviceName }}
{{- $serviceName = $traces.otel.serviceName -}}
{{- else if or $metrics.enabled $traces.enabled }}
{{- $serviceName = include "wallaby.fullname" . -}}
{{- end }}
{{- if $serviceName }}
- name: OTEL_SERVICE_NAME
  value: {{ $serviceName | quote }}
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
