import pymongo
import sqlite3
from sqlite3 import Error
from datetime import datetime, time


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
        timestamp_to_dt(row[5]/1000)


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


def main():
    database = 'reasonCodeStamp.db'
    conn = create_sqlite_connection(database)
    with conn:
        # select_by_rscode(conn, 'Fixture installation')
        # count_all(conn)
        test_time()


if __name__ == '__main__':
    main()
