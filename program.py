import pymongo
import sqlite3
from sqlite3 import Error
from datetime import datetime, time
import time as ctime
import pprint


def write_to_mongo(report_):
    client = pymongo.MongoClient()
    reports = client['universal-dashboard']['reports']
    reports.insert_one(report_)


report = {
    '_id': 201907,
    'off24': 23,
    'off18': 3,
    'setup': 12,
}


def create_sqlite_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def select_by_rscode(conn, rscode):
    cur = conn.cursor()
    cur.execute('SELECT * FROM mainTable WHERE rscode=?', (rscode,))
    rows = cur.fetchall()
    for row in rows:
        print(row)
        print(datetime.fromtimestamp(row[5]/1000))


def count_all(conn):
    cur = conn.cursor()
    cur.execute('SELECT COUNT (*) as count FROM mainTable')
    rows = cur.fetchall()
    for row in rows:
        print(row)
    print(rows[0][0])


def timestamp_to_dt(ts):
    dt = datetime.fromtimestamp(ts)
    print(dt)


def test_time():
    t1 = time.fromisoformat('04:23:01')
    t2 = time.fromisoformat('10:22:00')
    print(t1 > t2)
    dt1 = datetime(2019, 10, 11, 5, 10, 0)
    dt2 = datetime(2019, 10, 12, 3, 1, 0)
    print(dt1.time() > dt2.time())


time115 = time.fromisoformat('01:15:00')
time715 = time.fromisoformat('07:15:00')


# check whether should include this into the 18 hour shift report
def is_in_report_18(time_):
    return time_ < time115 or time_ > time715


def main():
    REPORT18 = 'Report 18'
    REPORT24 = 'Report 24'
    IDLE = 'Idle'
    SETUP_DRYRUN = 'SetupDryrun'
    MACHINING = 'Machining'
    OFF = 'Off'
    LOAD_UNLOAD = 'Loading /unloading'
    FIXTURE_INSTALLATION = 'Fixture installation'
    ERDT = 'Engineering repair development trials'
    PREVENTIVE_MAINTENANCE = 'Preventive maintenance'
    BREAK_DOWN = 'Break down'
    reports = {}
    all_reason_codes = [LOAD_UNLOAD, FIXTURE_INSTALLATION, ERDT, PREVENTIVE_MAINTENANCE, BREAK_DOWN]
    bar_key_18_hour_shift_exception = [ERDT, SETUP_DRYRUN, MACHINING]

    database = 'reasonCodeStamp.db'
    conn = create_sqlite_connection(database)
    with conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM mainTable ')  # all records
        rows = cur.fetchall()
        for row in rows:
            # print(row)
            dt = datetime.fromtimestamp(row[5]/1000)
            monthly_key = dt.year * 100 + dt.month
            daily_key = dt.year * 10000 + dt.month * 100 + dt.day
            # print(monthly_key, daily_key)
            reason_code = row[4]

            if reason_code in all_reason_codes:
                bar_key = reason_code
            else:
                if row[0] == 1:
                    bar_key = IDLE
                elif row[1] == 1:
                    bar_key = SETUP_DRYRUN
                elif row[2] == 1:
                    bar_key = MACHINING
                else:
                    bar_key = OFF

            if monthly_key not in reports:
                reports[monthly_key] = {
                    REPORT24: {
                    IDLE: 0,
                    SETUP_DRYRUN: 0,
                    MACHINING: 0,
                    OFF: 0,
                    LOAD_UNLOAD: 0,
                    FIXTURE_INSTALLATION: 0,
                    ERDT: 0,
                    PREVENTIVE_MAINTENANCE: 0,
                    BREAK_DOWN: 0,
                },
                    REPORT18: {
                    IDLE: 0,
                    SETUP_DRYRUN: 0,
                    MACHINING: 0,
                    OFF: 0,
                    LOAD_UNLOAD: 0,
                    FIXTURE_INSTALLATION: 0,
                    ERDT: 0,
                    PREVENTIVE_MAINTENANCE: 0,
                    BREAK_DOWN: 0,
                }}
            if daily_key not in reports:
                reports[daily_key] = {
                    REPORT24: {
                    IDLE: 0,
                    SETUP_DRYRUN: 0,
                    MACHINING: 0,
                    OFF: 0,
                    LOAD_UNLOAD: 0,
                    FIXTURE_INSTALLATION: 0,
                    ERDT: 0,
                    PREVENTIVE_MAINTENANCE: 0,
                    BREAK_DOWN: 0,
                },
                    REPORT18: {
                    IDLE: 0,
                    SETUP_DRYRUN: 0,
                    MACHINING: 0,
                    OFF: 0,
                    LOAD_UNLOAD: 0,
                    FIXTURE_INSTALLATION: 0,
                    ERDT: 0,
                    PREVENTIVE_MAINTENANCE: 0,
                    BREAK_DOWN: 0,
                }}
            # 24 hour shift reports
            reports[monthly_key][REPORT24][bar_key] = reports[monthly_key][REPORT24][bar_key] + 1
            reports[daily_key][REPORT24][bar_key] = reports[daily_key][REPORT24][bar_key] + 1
            # 18 hour shift reports
            if is_in_report_18(dt.time()) or bar_key in bar_key_18_hour_shift_exception:
                reports[monthly_key][REPORT18][bar_key] = reports[monthly_key][REPORT18][bar_key] + 1
                reports[daily_key][REPORT18][bar_key] = reports[daily_key][REPORT18][bar_key] + 1
    pprint.pprint(reports)


if __name__ == '__main__':
    start_time = ctime.time()
    main()
    print("--- %s seconds ---" % (ctime.time() - start_time))
