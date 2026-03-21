#!/usr/bin/env bash
# Activity by equipment name and facility
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions sensor_readings.equipment_name \
  --dimensions sensor_readings.facility_id \
  --measures sensor_readings.total_readings \
  --measures sensor_readings.unique_sensors \
  --measures sensor_readings.days_active \
  --order sensor_readings.total_readings:desc \
  --limit 20
