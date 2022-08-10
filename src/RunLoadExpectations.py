
import json
from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from src.DataValidationContext import GEDataValidationContext
from src.BatchRequest import getBatchRequest 
        

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from snowflake.snowpark.types import IntegerType, StringType, StructField,VariantType,StructType,BooleanType
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint import Checkpoint


def runExpectaionValidation(context,checkpointname,batchrequest,expectationsuitename,datasourcename):
    
    my_checkpoint_name = checkpointname
    checkpoint_config = {
                "name": my_checkpoint_name,
                "config_version": 1.0,
                "class_name": "SimpleCheckpoint",
                "run_name_template": "%Y%m%d-%H%M%S-my-pandas_run-name-template",
            }
            
    context.add_checkpoint(**checkpoint_config)

    # run expectation_suite against Pandas dataframe
    validation_result = context.run_checkpoint(
            checkpoint_name = my_checkpoint_name,
            validations=[
                {
                    "batch_request": batchrequest,
                    "expectation_suite_name": expectationsuitename,
                }
            ],
        )
    return validation_result
    
def loadValidationToDB(session,validationresult,tablename):
            
    # Defining the schema, creating the Snowpark DF and and writing the validation results to a table.
    schema = StructType([StructField("RunStatus", BooleanType()),StructField("RunId", VariantType()), StructField("RunValidation", VariantType())])

    df=session.create_dataframe([[validationresult.success, json.loads(str(validationresult.run_id)),json.loads(str(validationresult.list_validation_results()))]], schema)

    df.write.mode('append').saveAsTable(tablename)
    print(f" Loaded Validation Results...")

