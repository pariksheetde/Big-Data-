import cx_Oracle

# create Oracle DB connection
conn = cx_Oracle.connect("Dev/Dev@localhost:1521/prod")

# create a cursor
cur = conn.cursor()
SQL = """
CREATE TABLE dev.departments
(
    dept_id int primary key,
    dept_name varchar2(40) not null,
    loc_id int,
    FOREIGN KEY (loc_id) references locations(loc_id)
)
"""
cur.execute(SQL)
print("Table created successfully...")