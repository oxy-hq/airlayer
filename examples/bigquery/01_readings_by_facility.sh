#!/usr/bin/env bash
# Reading volume by facility and region
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions sensor_readings.facility_id \
  --dimensions sensor_readings.region \
  --measures sensor_readings.total_readings \
  --measures sensor_readings.unique_sensors \
  --order sensor_readings.total_readings:desc \
  --limit 10
