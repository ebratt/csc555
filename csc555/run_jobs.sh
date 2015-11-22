#!/bin/bash
# RUNNING ALL OBS
echo "#### RUNNING ALL JOBS ####" > run_jobs.log 2>&1;

# RUN EACH JOB
for script in `ls scripts/*.sh`; do
  echo "running ${script}" >> run_jobs.log 2>&1;
  ./scripts/$script >> run_jobs.log 2>&1;
  echo "completed running ${script}" >> run_jobs.log 2>&1;
done
