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
        SQL = """
        INSERT INTO Dev.locations values (:1, :2)
        """
        cur.execute(SQL, [80, 'Marseille'])
    except Exception as err:
        print("Error while inserting records:", err)
    else:
        print(f"Record inserted successfully {cur.rowcount}")
        connection.commit()
finally:
    cur.close()
    connection.close()