#!/usr/bin/env bash
python /opt/amit_scripts_python/aggregationStats.py $(date -d "-1 days" +"%Y-%m-%d")>>aggStats.log
