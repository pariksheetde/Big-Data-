import cx_Oracle

try:
    # create Oracle DB connection
    connection = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")
except Exception as err:
    print(f"Error while connecting to Oracle DB", err)
else:
    try:
        cur = connection.cursor()
        SQL_select = """select l.loc_id, l.loc_name from locations l order by 1 asc"""
        # SQL_truncate = """TRUNCATE TABLE city"""
        SQL_insert = """INSERT INTO city values (:1, :2)"""
        cur.execute(SQL_select)
        row = cur.fetchall()
        print(row)
        # cur.execute(SQL_truncate)
        cur.executemany(SQL_insert, row)
    except Exception as err:
            print(f"Error while fetching data", err)
    else:
        print(f"Record(s) returned: {cur.rowcount}")
        connection.commit()
finally:
    cur.close()
    connection.close()