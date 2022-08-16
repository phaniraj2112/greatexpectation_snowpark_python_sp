# Great Expectation With Snowflake Python Stored Procedures

### Introduction

Data validation is the most essential part of any data engineering pipeline or even machine learning pipelines these days. There are quite a few tools available which can help us in validating the data before it is used by repporting or any application. One such great tool is Great Expectations. Great Expectations helps you to know your data much better. Its a open source data validation tool  which can be used to validate the data before or after its loaded into your data warehouse, data lake or data lakehouse. There are lot of example you will find which talks about how to use Great Expectations. 

In this demonstation you will learn how to use [Great Expecations](https://docs.greatexpectations.io/docs/) to perform data validation natively inside Snowflake using *Python Stored Procedures*. 

### High level steps to validate data using Great Expectations(GE)
 -  Create GE context inside the code.
 - Providing DataSouce details along with the batch which has the DataFrame information.
 - Creating Expectation Suite. 
 - Creating all the required expectations.
 - Validating the expectations on the data defined in the batch.
 - Saving the validation reults in a Snowflake table.

There are two versions to demonstrate the implemetation:

- Single jupyter notebook which has the entire code to create the stored procedure. You can run <b>*GEwithPythonSP_SimpleVersion.ipynb*</b> notebook by making changes to the Snowflake configuration ,table details used in the code along with the expectations specific to your requirements. In this demo we have created only three expectations to demonstrate the implementation.

- GE implementation broken down into multiple python files. This allows to you just make changes to required python file and not touching the other part of the code. This allows you to have write better unit test cases and have better control on the changes you are deploying. You can run <b>*GE_PythonStoredProc.ipynb*</b> notebook which imports all the dependent python files required for the Python Stored Procedure.

