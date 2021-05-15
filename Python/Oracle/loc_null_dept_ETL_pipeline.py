import cx_Oracle

try:
    # create Oracle DB connection
    connection = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")
except Exception as err:
    print(f"Error while connecting to Oracle DB", err)
else:
    try:
        cur = connection.cursor()
        SQL_select = """select l.loc_id, d.dept_id, l.loc_name, d.dept_name from locations l left join departments d
        on l.loc_id = d.loc_id where d.dept_id is null order by 2 desc
        """
        SQL_truncate = """TRUNCATE TABLE loc_null_dept"""
        SQL_insert = """INSERT INTO loc_null_dept values (:1, :2, :3, :4)"""
        cur.execute(SQL_select)
        row = cur.fetchall()
        print(row)
        cur.execute(SQL_truncate)
        cur.executemany(SQL_insert, row)
    except Exception as err:
            print(f"Error while fetching data", err)
    else:
        print(f"Record(s) returned: {cur.rowcount}")
        connection.commit()
finally:
    cur.close()
    connection.close()