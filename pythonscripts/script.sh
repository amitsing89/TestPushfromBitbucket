#!/usr/bin/env bash
python /opt/amit_scripts_python/dataCheck.py $(date -d "-1 days" +"%Y-%m-%d") >> datacomparison.log


