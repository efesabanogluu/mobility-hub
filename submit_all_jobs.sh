#!/bin/bash

jobs=(
  preprocessing_job.py
  driver_job.py
  passengers_job.py
  h3_metrics_job.py
  vehicle_type_job.py
)

for job in "${jobs[@]}"; do
  echo "Submitting job: $job"
  docker exec -d jobmanager flink run -py /opt/flink/jobs/$job --pyFiles /opt/flink/jobs/helpers.py
done

echo "✅ All jobs submitted. Check http://localhost:8081"
