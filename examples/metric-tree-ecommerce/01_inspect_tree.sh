#!/bin/bash
# Show the full metric tree — component + driver edges across all views
cd "$(dirname "$0")"
../../target/debug/airlayer inspect --metric-tree
