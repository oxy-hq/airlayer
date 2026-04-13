#!/bin/bash
# Show the full funnel metric tree — component edges from conversion rates,
# driver edges from new_active_listings to key levers
cd "$(dirname "$0")"
airlayer inspect --metric-tree
