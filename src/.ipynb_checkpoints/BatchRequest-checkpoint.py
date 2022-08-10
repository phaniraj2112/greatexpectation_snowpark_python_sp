from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


from src.DataValidationContext import GEDataValidationContext

def getBatchRequest(context,datasourcename,pandasDF):

    # print(context.getContext())

    #Creating the batch request which will be used while running the checkpoint
    batch_request = RuntimeBatchRequest(
                                datasource_name=datasourcename,
                                    data_connector_name="default_runtime_data_connector_name",
                                    data_asset_name="PandasData",  # This can be anything that identifies this data_asset for you
                                    runtime_parameters={"batch_data": pandasDF},  # df is your dataframe, you have created above.
                                    batch_identifiers={"default_identifier_name": f'default_identifier_{datasourcename}'},
                                    )
    return batch_request

