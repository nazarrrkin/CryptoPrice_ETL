import psycopg2
from config import dbname, user, password, host

class Database:
    def __init__(dbname, user, password, host):
        connection = psycopg2.connect(dbname = dbname,
                                user = user,
                                password = password,
                                host = host)
        cursor = connection.cursor()
        
    def initiation(cursor):
        cursor.execute("""CREATE TABLE """)
