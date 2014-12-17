import settings
import AzureUtil

table_service = AzureUtil.AzureUtil().table_service


def purge():
    outlet = '00-26-6F-20-0C-A4'
    purge_minutes(outlet)
    # purge_hour(outlet)
    # purge_minutes(outlet)
    # purge_insight_history(outlet)
    print 'end'


def purge_minutes(outlet):
    return purge_entity('TableUsageMinutes', outlet)


def purge_hour(outlet):
    return purge_entity('TableUsageHours', outlet)


def purge_week(outlet):
    return purge_entity('TableUsageWeeks', outlet)


def purge_insight_history(outlet):
    return purge_entity('TableOutletInsightHistories', outlet)


def purge_entity(table, outlet):
    entities = table_service.query_entities(table, settings.data_upload_azure_storage_condition.format(outlet))
    while entities:
        print 'delete entities {0}'.format(len(entities))
        for entity in entities:
            print '<{0}, {1}>'.format(entity.PartitionKey, entity.RowKey)
            table_service.delete_entity(table, entity.PartitionKey, entity.RowKey, content_type='application/atom+xml')

        if hasattr(entities, 'x_ms_continuation'):
            entities = table_service.query_entities(table,
                                                    settings.data_upload_azure_storage_condition.format(outlet),
                                                    next_partition_key=entities.x_ms_continuation['nextpartitionkey'],
                                                    next_row_key=entities.x_ms_continuation['nextrowkey'])


if __name__ == '__main__':
    purge()
