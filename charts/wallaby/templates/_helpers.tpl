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

{{- define "wallaby.workerServiceAccountName" -}}
{{- if .Values.workerServiceAccount.create -}}
{{- default (printf "%s-worker" (include "wallaby.fullname" .)) .Values.workerServiceAccount.name -}}
{{- else -}}
{{- default "default" .Values.workerServiceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "wallaby.grpcHealthProbeImage" -}}
{{- printf "%s@%s" .Values.tests.grpcHealthProbeImage.repository (.Values.tests.grpcHealthProbeImage.digest | trim) -}}
{{- end -}}

{{- define "wallaby.otelEnv" -}}
{{- $metrics := .Values.observability.metrics -}}
{{- $traces := .Values.observability.traces -}}
{{- if and $metrics.enabled $metrics.otel.endpoint }}
- name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  value: {{ $metrics.otel.endpoint | quote }}
- name: WALLABY_OTEL_METRICS_INSECURE
  value: {{ $metrics.otel.insecure | toString | quote }}
- name: OTEL_EXPORTER_OTLP_METRICS_PROTOCOL
  value: {{ $metrics.otel.protocol | default "grpc" | quote }}
- name: OTEL_METRICS_EXPORTER
  value: "otlp"
{{- if $metrics.otel.interval }}
- name: WALLABY_OTEL_METRICS_INTERVAL
  value: {{ $metrics.otel.interval | quote }}
{{- end }}
{{- end }}
{{- if and $traces.enabled $traces.otel.endpoint }}
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: {{ $traces.otel.endpoint | quote }}
- name: WALLABY_OTEL_TRACES_INSECURE
  value: {{ $traces.otel.insecure | toString | quote }}
- name: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL
  value: {{ $traces.otel.protocol | default "grpc" | quote }}
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

{{- define "wallaby.snowflakeWorkerEnv" -}}
- name: WALLABY_WORKER_SNOWFLAKE_ENABLED
  value: {{ .Values.snowflake.enabled | toString | quote }}
- name: WALLABY_WORKER_SNOWFLAKE_ACCOUNT
  value: {{ .Values.snowflake.account | quote }}
- name: WALLABY_WORKER_SNOWFLAKE_USER
  value: {{ .Values.snowflake.user | quote }}
- name: WALLABY_WORKER_SNOWFLAKE_HOST
  value: {{ .Values.snowflake.host | quote }}
- name: WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE
  value: {{ .Values.snowflake.privateKeyFile | quote }}
- name: WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ default (printf "%s-snowflake-policy" (include "wallaby.fullname" .)) .Values.snowflake.streamingRest.policyConfigMapName | quote }}
      key: {{ .Values.snowflake.streamingRest.policyConfigMapKey | quote }}
{{- end -}}
