DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')
python /opt/amit_scripts_python/conversionData.py $YEAR $MONTH $DAY
