#!/bin/bash

# Define the name of the virtual environment
ENV_NAME=udf_test

# Create the virtual environment
python3 -m venv $ENV_NAME

# Activate the virtual environment
source $ENV_NAME/bin/activate

cp ./dozer-tests/python_udf/python_udf.py udf_test
