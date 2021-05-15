import cx_Oracle

try:
    # create Oracle DB connection
    conn = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")
except Exception as err:
    print("Error while connecting to Oracle DB...", err)
else:
    try:
        cur = conn.cursor()
        SQL = """INSERT INTO Dev.departments values (:1, :2, :3)"""
        data = [(170, 'Power BI Developer', 15),(180, 'Tableau Developer', 15)]
        cur.executemany(SQL, data)
    except Exception as err:
        print("Error while inserting records", err)
    else:
        print(f"Record inserted successfully {cur.rowcount}")
        conn.commit()
finally:
    cur.close()
    conn.close()