#!/bin/bash
# Show the full metric tree — component + driver edges across all views
cd "$(dirname "$0")"
airlayer inspect --metric-tree
