# Great Expectation With Snowpark Python Stored Procedures

### Introduction

Data validation is the most essential part of any data engineering pipeline or even machine learning pipelines these days. There are quite a few tools available which can help us in validating the data before it is used by reporting or any by application. One such great tool is Great Expectations. Great Expectations helps you to get better insights into your data. Its a open source data validation tool which can be used to validate the data before or after its loaded into your data warehouse, data lake or data lakehouse. 

In this demonstation you will learn how to use [Great Expecations](https://docs.greatexpectations.io/docs/) to perform data validation natively inside Snowflake using *Python Stored Procedures*. 

### High level steps to validate data using Great Expectations(GE)
 - Create GE context inside the code.
 - Providing DataSouce details along with the batch which has the DataFrame information.
 - Creating Expectation Suite. 
 - Creating all the required expectations.
 - Validating the expectations on the data .
 - Saving the validation results in a Snowflake table.

 All of the above steps will be performed inside a Snowpark Python Stored procedure.

There are two versions to demonstrate the implementation:

- Single jupyter notebook which has the entire code to create the stored procedure. You can run <b>*GEwithPythonSP_SimpleVersion.ipynb*</b> notebook by making changes to the Snowflake configuration ,table details used in the code along with the expectations specific to your requirements. In this demo we have created only three expectations to demonstrate the implementation.

- GE implementation broken down into multiple python files. This allows you to just make changes to  the required python file and not touch the other parts of the code. This allows you to write better unit test cases and have better control on the changes you are deploying. You can run <b>*GE_PythonStoredProc.ipynb*</b> notebook which imports all the dependent python files required for the Python Stored Procedure. Once the stored procedure is created, you can run the SP as mentioned in the notebook which will load the validation results to table name passed in as the parameter to the SP. You need to update the Expectations.py file so that the expectations are specific to your tables and usecase. 


### More
This framework can be extended by passing in multiple batch requests at run time as the checkpoint runs the expectation guite against the batch that you can pass at run time.

You can create multiple dataframe and run the expectations against each of this DataFrame also using different expectation suites that you have created based on the data and business requirements.