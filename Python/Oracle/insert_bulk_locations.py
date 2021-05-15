import cx_Oracle

try:
    # create Oracle DB connection
    connection = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")
    print(f"Connecting to Oracle DB: Version {connection.version}")
except Exception as err:
    print("Error while connecting to Oracle DB...", err)
else:
    try:
        cur = connection.cursor()
        SQL = """INSERT INTO Dev.locations values (:1, :2)"""
        data = [(70, 'Nice'),(75, 'Lille')]
        cur.executemany(SQL, data)
    except Exception as err:
        print("Error while inserting records")
    else:
        print("1 record inserted successfully")
        connection.commit()
finally:
    cur.close()
    connection.close()