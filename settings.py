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
pot_insight_db_query = 'select o.macaddress, his.startdate, his.enddate ' \
    'from BosonDB.dbo.OutletHistories his, BosonDB.dbo.Outlets o ' \
    'where his.outletid = o.id ' \
    'and his.devicecategoryid = 9 '
pot_insight_azure_storage_condition = "PartitionKey eq '{0}' and Time ge datetime'{1}' and Time lt datetime'{2}'"
pot_insight_start_date = datetime(2014, 7, 21, 8)

# Configuration for data_upload_status
data_upload_status_db_query = "select * from Outlets"
data_upload_azure_storage_condition = "PartitionKey eq '{0}'"

# Configuration for money_saving
money_saving_azure_storage_condition = "PartitionKey eq '{0}' and Timestamp ge datetime'{1}' and Timestamp lt datetime'{2}'"
money_saving_file_path = '$file_path'

# Configuration for clone_usage_data
clone_usage_data_azure_storage_condition = "PartitionKey eq '{0}' and Time ge datetime'{1}' and Time lt datetime'{2}'"