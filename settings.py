from datetime import datetime

# Azure settings
azure_storage_account = '$storage_account'
azure_storage_account_key = '$storage_account_key'
azure_mssql_server = '$db_server'
azure_mssql_user = '$db_user'
azure_mssql_pwd = '$db_password'
azure_mssql_db = '$db_name'

# Configuration for pot_insight
pot_insight_file_path = '$file_path'
pot_insight_db_query = '$sql_statement'
pot_insight_azure_storage_condition = "$azure_condition"
pot_insight_start_date = datetime(2014, 7, 21, 8)

# Configuration for data_upload_status
data_upload_status_db_query = "$sql_statement"
data_upload_azure_storage_condition="PartitionKey eq '{0}'"