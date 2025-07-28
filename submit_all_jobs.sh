#!/bin/bash

# List of Python-based Flink job scripts to be submitted
jobs=(
  preprocessing_job.py
  driver_job.py
  passengers_job.py
  h3_metrics_job.py
  vehicle_type_job.py
)

# Iterate through each job and submit it to the Flink JobManager via docker exec
for job in "${jobs[@]}"; do
  echo "Submitting job: $job"
  
  # -d runs in detached mode
  # --pyFiles passes helper modules required for the job (e.g., helpers.py)
  docker exec -d jobmanager flink run -py /opt/flink/jobs/$job --pyFiles /opt/flink/jobs/helpers.py
done

# Inform user that job submission is complete and where to view them
echo "✅ All jobs submitted. Check http://localhost:8081"
