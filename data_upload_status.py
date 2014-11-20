# coding: UTF-8
"""
Check the state of the usage data uploaded.
"""
import pytz
import settings
import AzureUtil

from datetime import datetime

table_service = AzureUtil.AzureUtil().table_service

has_entity = False
date_format = '%Y-%m-%d'


def check_upload_usage():

    conn = AzureUtil.AzureUtil().connection

    cursor = conn.cursor(as_dict=True)
    cursor.execute(settings.data_upload_status_db_query)

    for outlet in cursor:
        has_entity = False
        has_usage_data = check_hour(outlet) or check_minutes(outlet) or check_week(outlet)

        delta = datetime.utcnow() - outlet['CreatedDate']
        if (not has_usage_data) and has_entity:
            # TutorPowerOff
            print u'[{0}, {1}, {2}, {3}]'.format(outlet['MacAddress'], u'電源關閉?',
                                                 delta.days, outlet['CreatedDate'])
        elif not has_usage_data:
            # TutorManualUpload
            print u'[{0}, {1}, {2}, {3}]'.format(outlet['MacAddress'], u'沒有上傳資料',
                                                 outlet['CreatedDate'].strftime(date_format), delta.days)


def check_minutes(outlet):
    return check_usage_entity('TableUsageMinutes', outlet)
    

def check_hour(outlet):
    return check_usage_entity('TableUsageHours', outlet)
    

def check_week(outlet):
    return check_usage_entity('TableUsageWeeks', outlet)


def check_usage_entity(table, outlet):
    entities = table_service.query_entities(table, settings.data_upload_azure_storage_condition.format(outlet['MacAddress']))
    has_volt = False
    while entities:
        for entity in entities:
            if entity.Watt > 0:
                delta = entity.Timestamp - outlet['CreatedDate'].replace(tzinfo=pytz.utc)
                if outlet['DeviceCategoryId'] != 21:
                    # TutorFirstUploadSuccess
                    print u'[{0}, {1}, {2}, {3}]'.format(outlet['MacAddress'], u'上傳成功(有類別)',
                                                        outlet['CreatedDate'].strftime(date_format), delta.days)
                    return True
                else:
                    # TutorFirstUploadSuccessNoCategory
                    print u'[{0}, {1}, {2}, {3}]'.format(outlet['MacAddress'], u'上傳成功(沒類別)',
                                                         outlet['CreatedDate'].strftime(date_format), delta.days)
                    return True
            elif entity.Voltage > 0:
                has_volt = True

            has_entity = True

        if hasattr(entities, 'x_ms_continuation'):
            entities = table_service.query_entities(table, "PartitionKey eq '" + outlet['MacAddress'] + "'",
                                                    next_partition_key=entities.x_ms_continuation['nextpartitionkey'],
                                                    next_row_key=entities.x_ms_continuation['nextrowkey'])
        else:
            break

    if has_volt:
        delta = datetime.utcnow() - outlet['CreatedDate']
        # TutorUploadZeroValue
        print u'[{0}, {1}, {2}, {3}]'.format(outlet['MacAddress'], u'上傳資料對特為 0',
                                             outlet['CreatedDate'].strftime(date_format), delta.days)
        return True

    return False


if __name__ == '__main__':
    check_upload_usage()