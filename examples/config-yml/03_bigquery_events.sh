#!/usr/bin/env bash
# Query events — datasource: analytics maps to bigquery in config.yml
# Note: backtick quoting in output confirms BigQuery dialect
set -euo pipefail
cd "$(dirname "$0")"

airlayer query -c config.yml \
  --dimensions events.event_type \
  --measures events.event_count \
  --measures events.unique_users \
  --order events.event_count:desc
