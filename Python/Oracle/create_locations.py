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
        CREATE TABLE dev.locations
        (
            loc_id int primary key,
            loc_name varchar2(20) not null
        )
        """
        cur.execute(SQL)
    except Exception as err:
        print("Error while creating Table:", err)
    else:
        print("Table Created successfully")
        connection.commit()
finally:
    cur.close()
    connection.close()