import cx_Oracle

try:
    # create Oracle DB connection
    connection = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")
    print(f"Connected to Oracle DB: Version {connection.version}")
except Exception as err:
    print("Error while connecting to Oracle DB...", err)
else:
    try:
        cur = connection.cursor()
        SQL = """
        INSERT INTO Dev.departments values (:1, :2, :3)
        """
        cur.execute(SQL, [160, 'Azure PySpark Developer', 15])
    except Exception as err:
        print("Error while inserting records:", err)
    else:
        print(f"Record inserted successfully: {cur.rowcount}")
        print()
        connection.commit()
finally:
    cur.close()
    connection.close()