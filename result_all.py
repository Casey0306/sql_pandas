from psycopg2 import Error
from pandas import DataFrame
import psycopg2
import os


try:
    connection = psycopg2.connect(user=os.getenv('DB_USER'),
                                  password=os.getenv('DB_PASS'),
                                  host=os.getenv('DB_HOST'),
                                  port=os.getenv('DB_PORT'),
                                  database=os.getenv('DB_NAME'))

    cursor = connection.cursor()
    cursor.execute("SELECT communications.communication_id,"
                   " communications.site_id,"
                   " communications.visitor_id,"
                   " communications.date_time,"
                   " (SELECT "
                   "sessions.visitor_session_id FROM sessions"
                   " WHERE sessions.site_id = communications.site_id"
                   " and sessions.visitor_id"
                   " = communications.visitor_id and sessions.date_time IN"
                   " (SELECT MAX(sessions.date_time)"
                   " FROM sessions"
                   " WHERE sessions.site_id=communications.site_id"
                   " and sessions.visitor_id=communications.visitor_id"
                   " and sessions.date_time < communications.date_time)),"
                   " (SELECT MAX(sessions.date_time) "
                   "FROM sessions"
                   " WHERE sessions.site_id = communications.site_id"
                   " and sessions.visitor_id = communications.visitor_id"
                   " and sessions.date_time < communications.date_time),"
                   " (SELECT sessions.campaign_id FROM sessions"
                   " WHERE sessions.site_id = communications.site_id"
                   " and sessions.visitor_id = communications.visitor_id"
                   " and sessions.date_time IN"
                   " (SELECT MAX(sessions.date_time) "
                   "FROM sessions"
                   " WHERE sessions.site_id=communications.site_id "
                   "and sessions.visitor_id=communications.visitor_id "
                   "and sessions.date_time < communications.date_time)), "
                   "(SELECT count(*) FROM sessions "
                   "WHERE sessions.site_id = communications.site_id "
                   "and sessions.visitor_id = communications.visitor_id "
                   "and sessions.date_time < communications.date_time)"
                   "  FROM communications;")
    record = cursor.fetchall()

    df = DataFrame(record)
    df.columns = ('communication_id',
                  'site_id',
                  'visitor_id',
                  'communication_date_time',
                  'visitor_session_id',
                  'session_date_time',
                  'campaign_id',
                  'row_n')
    df.to_csv("result.csv", encoding='utf-8', index=False)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
