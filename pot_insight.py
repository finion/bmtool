"""
Analyze energy usage pattern of pot.
"""
import settings
import AzureUtil

from datetime import datetime, timedelta


def run_insight():
    utctime_format = '%Y-%m-%dT%H:%M:%S'
    date_format = '%Y-%m-%d'

    table_service = AzureUtil.AzureUtil().table_service

    conn = AzureUtil.AzureUtil().connection

    cursor = conn.cursor()
    cursor.execute(settings.pot_insight_db_query)

    li = []
    for result in cursor:
        # result : (mac address, start date, end date)
        li.append(result)

    sd = settings.pot_insight_start_date
    with open(settings.pot_insight_file_path, 'w+') as f:
        while sd < datetime.utcnow():
            ed = sd + timedelta(days=7) # query data by week
            f.write('date {0} ~ {1}\n'.format(sd.strftime(date_format), (ed - timedelta(days=1)).strftime(date_format)))
            f.write('   outlet, card, vacuum(5 < w < 30), tradition(30 <= w < 100)\n')

            for outlet in li:
                """
                If end date is None mean the outlet still use pot as category,
                and analyzing data when the pot chosen in the period of week.
                """
                #
                if ((outlet[2] is None) or (outlet[2] > ed)) and outlet[1] < ed:
                    print 'query azure {0}'.format(outlet)
                    condition = settings.pot_insight_azure_storage_condition.format(
                        outlet[0], sd.strftime(utctime_format), ed.strftime(utctime_format))

                    print condition
                    usages = table_service.query_entities('TableUsageHours', condition)
                    usages = [x for x in usages if 5 < x.Watt <= 650]
                    if usages:
                        card = get_card(usages)
                        print 'card: {0}'.format(card)
                        # 5 < watt < 30 : standby energy by vacuum pot
                        standby_usage_vacuum = [x for x in usages if 5 < x.Watt <= 30]

                        # 30 < watt <= 100 : standby energy by traditional pot
                        standby_usage = [x for x in usages if 30 < x.Watt <= 100]

                        f.write('   {0}, {1}, {2}, {3}\n'.format(
                            outlet[0], card, len(standby_usage_vacuum), len(standby_usage)
                        ))
                    else:
                        f.write('   {0} no upload data\n'.format(outlet[0]))
            sd = ed
            f.write('\n')
    f.close()


def get_card(usages):
    max_watt = max(usage.Watt for usage in usages)
    print max_watt
    if 300 <= max_watt < 440:
        return '3L'
    elif 440 <= max_watt < 540:
        return '4L'
    elif 540 <= max_watt < 650:
        return '5L'
    elif max_watt < 300:
        return 'no card'
    else:
        return 'nothing'


if __name__ == '__main__':
    run_insight()