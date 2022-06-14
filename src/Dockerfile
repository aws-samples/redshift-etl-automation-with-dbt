# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

FROM public.ecr.aws/lambda/python:3.8

ENTRYPOINT ["/bin/bash", "-l", "-c"]

ADD dbt-project /dbt-project

# Update and install system packages
RUN yum update kernel -y
RUN yum update -y
RUN pip install -U pip

# Install DBT
RUN pip install --no-cache-dir dbt-core
RUN pip install --no-cache-dir dbt-redshift

WORKDIR /dbt-project
RUN chmod +x run-dbt.sh
RUN dbt deps --profiles-dir .

