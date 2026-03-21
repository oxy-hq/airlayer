#!/usr/bin/env bash
# Filter to pressure readings, broken down by facility and region
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d bigquery \
  --dimensions sensor_readings.facility_id \
  --dimensions sensor_readings.region \
  --measures sensor_readings.total_readings \
  --measures sensor_readings.alert_count \
  --filter sensor_readings.sensor_type:equals:pressure \
  --order sensor_readings.total_readings:desc \
  --limit 15
