import pandas as pd

import sys
import json
import platform
import os,requests
from pathlib import Path
import glob

from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from src.DataValidationContext import GEDataValidationContext
from src.BatchRequest import getBatchRequest 
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


def createExpectationSuite(context,suitename):
    context.create_expectation_suite(
    expectation_suite_name=suitename, overwrite_existing=True)
    
    
def createExpectations(context,suitename,local_batch_request,pandasdataframe):
    
    # Creating the validator which takes the batch request and expectation suite name
    validator = context.get_validator(
        batch_request=local_batch_request, expectation_suite_name=suitename
    )
    
    # Creating new validations based on data knowledge 
    validator.expect_column_values_to_be_in_set("TAXI_TYPE",["yellow","green"])
    validator.expect_column_min_to_be_between("VENDOR_ID",1,10)
    validator.expect_column_mean_to_be_between("VENDOR_ID",2,14)
    
    #Saving the expectation 
    validator.save_expectation_suite(discard_failed_expectations=False)

