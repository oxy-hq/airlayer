#!/usr/bin/env bash
# Metrics by sensor type, filtered to a specific facility
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d bigquery \
  --dimensions sensor_readings.sensor_type \
  --measures sensor_readings.total_readings \
  --measures sensor_readings.alert_count \
  --measures sensor_readings.avg_reading_value \
  --filter sensor_readings.facility_id:equals:fac_north_01 \
  --order sensor_readings.total_readings:desc
