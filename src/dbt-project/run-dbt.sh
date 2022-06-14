#!/bin/bash
date
echo "This job creates a database using a seed and then runs the dbt job"
echo "jobId: $AWS_BATCH_JOB_ID"
echo "jobQueue: $AWS_BATCH_JQ_NAME"
echo "computeEnvironment: $AWS_BATCH_CE_NAME"

if [ $# -eq 0 ]; then
    echo "No models were specified. Executing all models"
    dbt run --profiles-dir .
else
    echo "Executing only specified models"
    dbt run --profiles-dir . -m $@
fi
