import pandas as pd

import sys
import json
import platform
import os,requests
from pathlib import Path
import glob

from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
        
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from snowflake.snowpark.types import IntegerType, StringType, StructField,VariantType,StructType,BooleanType
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults



class GEDataValidationContext():
    
    def __init__(self,sf_datasourcename):
        
        self.datasource_name = sf_datasourcename

        # create data context
        # data_context_config = DataContextConfig( store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=sf_rootdirectory) )
        # context = BaseDataContext(project_config = data_context_config)
        
        data_context_config = DataContextConfig(
                        datasources={
                            "dataframe_datasource": DatasourceConfig(
                                class_name="PandasDatasource",
                                batch_kwargs_generators={
                                    "subdir_reader": {
                                        "class_name": "SubdirReaderBatchKwargsGenerator",
                                        "base_directory": "/tmp/great_expectation/",
                                    }
                                },
                            )
                        },
                        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/tmp/great_expectation"),
                        )
                   
            # Creating the GE context here
        context = BaseDataContext(project_config=data_context_config)
        
        
        '''
        Providing the datasource details which here is the pandas DF. We define the actual DF in the batch request which is defined after creating the DS
        '''

        datasource_config = {
        "name": self.datasource_name,
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
                }

        # Adding the DS to the context
        context.add_datasource(**datasource_config)
        
        self.context = context
    
    def getContext(self):
        return self.context