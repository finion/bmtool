#!/usr/bin/python
# coding: UTF-8

import settings
import AzureUtil
from datetime import datetime, timedelta
import time

outlet_schedule_cache = {}
cat_standby = {9: [70, 5], 3: [50, 3], 4: [100, 3], 10: [60, 3], 18: [60, 3]}
f = open(settings.money_saving_file_path, 'w')
utctime_format = '%Y-%m-%dT%H:%M:%S'
schedule_format = '%H:%M:%S'
date_format = '%Y-%m-%d'


def get_saving_money():
    conn = AzureUtil.AzureUtil().connection

    cursor = conn.cursor(as_dict=True)
    sql = 'select o.macaddress as mac, o.devicecategoryid as catid, sch.starttime as starttime, ' \
          'sch.endtime as endtime, o.id as id, sch.repeatdow as repeatdow, o.electricityrate as rate  ' \
          'from Outlets o, OutletSchedules sch ' \
          'where o.id = sch.outletid'
    cursor.execute(sql)

    sql2 = 'select his.startdate as startdate, his.enddate as enddate, ' \
           'his.devicecategoryid as catid, cat.name as cat_name ' \
           'from OutletHistories his, DeviceCategories cat ' \
           'where his.outletid = {0} ' \
           'and his.devicecategoryid = cat.id'

    for outlet in cursor:
        if (outlet['mac'], outlet['id']) in outlet_schedule_cache:
            print 'append on {0} - rate {1}'.format(outlet['mac'], outlet['rate'])
            outlet_schedule_cache[(outlet['mac'],
                                   outlet['id'],
                                   outlet['rate'])].append((outlet['starttime'],
                                                          outlet['endtime'],
                                                          outlet['repeatdow']))
        else:
            outlet_schedule_cache[(outlet['mac'],
                                   outlet['id'],
                                   outlet['rate'])] = [(outlet['starttime'],
                                                        outlet['endtime'],
                                                        outlet['repeatdow'])]

    i = 0
    for outlet_schedule in outlet_schedule_cache:
        conn2 = AzureUtil.AzureUtil().connection
        cursor2 = conn2.cursor(as_dict=True)
        cursor2.execute(sql2.format(outlet_schedule[1]))

        for his in cursor2:
            upper = 100
            lower = 3
            standby_hours = 0
            total_standby_hours = 0

            if his['catid'] in cat_standby:
                l = cat_standby[his['catid']]
                upper = l[0]
                lower = l[1]

            startdate = his['startdate']
            enddate = his['enddate'] if his['enddate'] is not None else datetime.utcnow()

            standby = guess_hour_standby(outlet_schedule[0], upper, lower, 3, startdate, enddate)

            saved = 0
            schedules = []

            schs = outlet_schedule_cache[outlet_schedule]

            for sch in schs:
                if sch[0] < sch[1]:
                    delta = sch[1] - sch[0]
                else:
                    delta = (sch[1] + timedelta(days=1)) - sch[0]

                days = len(sch[2].split(',')) - 1
                operation_hours = (delta.seconds / 3600.)
                standby_hours += 24 - operation_hours
                schedules.append((sch[0].strftime(schedule_format),
                                  sch[1].strftime(schedule_format),
                                  sch[2],
                                  operation_hours))
                total_standby_hours = standby_hours * days
                saved += (total_standby_hours * standby) / 1000 * outlet_schedule[2] * 52

            print 'saved {0}, rate {1}'.format(saved, outlet_schedule[2])
            if saved >= 1:
                i += 1
                f.write('{0}. {1} {2} ~ {3}\n'.format(i,
                                                      outlet_schedule[0],
                                                      startdate.strftime(date_format),
                                                      enddate.strftime(date_format)))
                for sch_tuple in schedules:
                    f.write('    {0} ~ {1} | {2} | hr: {3:.2f}\n'.format(sch_tuple[0],
                                                                         sch_tuple[1],
                                                                         sch_tuple[2],
                                                                         sch_tuple[3]))

                print '    (standby, cat, saved, hours of week) => \n' \
                      '    ({0}, {1} watt, NT {2}, {3} hr)\n'.format(standby,
                                                                     his['cat_name'],
                                                                     saved,
                                                                     total_standby_hours)
                f.write('    (category, standby watt, standby hour (a week) , saved (year)) => \n'
                        '    ({0}, {1:.2f} watt, {2:.2f} hr, NT {3:.0f}/y)\n\n'.format(his['cat_name'],
                                                                                       standby,
                                                                                       total_standby_hours,
                                                                                       saved))


def guess_hour_standby(mac, upper_watt, lower_watt, consecutive_hrs, startdate, enddate):
    print 'running {0}'.format(mac)
    table_service = AzureUtil.AzureUtil().table_service
    entities = table_service.query_entities('TableUsageHours',
                                            settings.money_saving_azure_storage_condition.format(mac,
                                                                                                 startdate.strftime(utctime_format),
                                                                                                 enddate.strftime(utctime_format)))

    pct = 0.1
    found_watts = []
    tmp_watts = []
    prev_watt = -1

    while entities:
        print 'entity count: {0}'.format(len(entities))

        for entity in entities:
            if lower_watt <= entity.Watt <= upper_watt:
                if prev_watt == -1 or ((prev_watt * (1 - pct)) <= entity.Watt <= (prev_watt * (1 + pct))):
                    tmp_watts.append(entity.Watt)
                else:
                    if len(tmp_watts) >= consecutive_hrs:
                        found_watts.append(sum(tmp_watts) / len(tmp_watts))

                    prev_watt = -1
                    del tmp_watts[:]
                    tmp_watts.append(entity.Watt)

                prev_watt = entity.Watt
            else:
                if len(tmp_watts) >= consecutive_hrs:
                    found_watts.append(sum(tmp_watts) / len(tmp_watts))

                prev_watt = -1
                del tmp_watts[:]

        if hasattr(entities, 'x_ms_continuation'):
            entities = table_service.query_entities('TableUsageHours',
                                                    settings.money_saving_azure_storage_condition.format(mac,
                                                                                                         startdate.strftime(utctime_format),
                                                                                                         enddate.strftime(utctime_format)),
                                                    next_partition_key=entities.x_ms_continuation['nextpartitionkey'],
                                                    next_row_key=entities.x_ms_continuation['nextrowkey'])
        else:
            break

    if len(found_watts) > 0:
        found = -1
        for watt in found_watts:
            if found == -1:
                found = watt
            elif watt < found:
                found = watt

        print 'found watt {0}'.format(found)
        return found
    else:
        print 'standby not found'
        return -1

if __name__ == '__main__':
    start = time.time()
    get_saving_money()
    end = time.time()
    print 'spends {0} sec'.format(end - start)