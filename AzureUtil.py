"""
Utility class deal with Microsoft Azure
"""
import settings
import pymssql

from azure.storage import TableService


class AzureUtil:

    def __init__(self):
        self.table_service = TableService(
            account_name=settings.azure_storage_account,
            account_key=settings.azure_storage_account_key
        )
        self.connection = pymssql.connect(
            settings.azure_mssql_server,
            settings.azure_mssql_user,
            settings.azure_mssql_pwd,
            settings.azure_mssql_db
        )