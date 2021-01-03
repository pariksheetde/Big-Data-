import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":
    print("Employee Details")

# set the SparkSession
spark = SparkSession.builder.appName("Employee Details").master("local[3]").getOrCreate()

# define schema for games location.csv
location_schema = StructType(
                          [
                           StructField("location_id", IntegerType(), False),
                           StructField("street_address", StringType(), False),
                           StructField("postal_code", IntegerType(), False),
                           StructField("city", StringType(), False),
                           StructField("state_province", StringType(), False),
                           StructField("country_id", StringType(), False)
                           ]
                         )

# read the datafile from the location
locations = (spark.read.option("header", "True")
         .option("schema", location_schema)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\locations.csv")
         .select("location_id", "street_address", "postal_code", "city", "state_province", "country_id")
         )
locations.show(3, truncate=False)

# define schema for departments.csv
departments_schema = StructType(
                          [
                           StructField("department_id", IntegerType(), False),
                           StructField("department_name", StringType(), False),
                           StructField("manager_id", IntegerType(), False),
                           StructField("location_id", IntegerType(), False)
                           ]
                         )

# read the datafile from the departments
departments = (spark.read.option("header", "True")
         .option("schema", departments_schema)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\departments.csv")
         .select("department_id", "department_name", "manager_id", "location_id")
         )
departments.show(3, truncate=False)

# define schema for employees.csv
departments_schema = StructType(
                          [
                           StructField("employee_id", IntegerType(), False),
                           StructField("first_name", StringType(), False),
                           StructField("last_name", IntegerType(), False),
                           StructField("email", IntegerType(), False),
                           StructField("phone_number", IntegerType(), False),
                           StructField("hire_date", DateType(), False),
                           StructField("job_id", StringType(), False),
                           StructField("salary", DoubleType(), False),
                           StructField("commission_pct", IntegerType(), False),
                           StructField("manager_id", IntegerType(), False),
                           StructField("department_id", IntegerType(), False)
                           ]
                         )

# read the datafile from the departments
employees = (spark.read.option("header", "True")
         .option("schema", departments_schema)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\employees.csv")
         .select("employee_id", "first_name", "last_name", "email", "phone_number", "hire_date", "job_id", "salary", "commission_pct", "manager_id", "department_id")
         )
employees.show(3, truncate=False)

# join locations and departments
join_loc_dept = locations.join(departments, on="location_id", how="inner") \
                         .select("location_id", "department_id", "department_name", "city", "country_id")

join_loc_dept_emp = join_loc_dept.join(employees, on="department_id", how="inner") \
                                 .selectExpr("employee_id as emp_id", "first_name as f_name".lower(), "last_name as l_name".lower(),
                                             "email as email_id".lower(),
                                             "hire_date", "location_id as loc_id", "department_id as dept_id", "salary") \
                                 .sort(["employee_id"], ascending = True)

# create temp table
SQL_tab = join_loc_dept_emp.createOrReplaceTempView("employee_dtls")

# query from temp table
SQL_qry = spark.sql("""select 
                       emp_id, f_name, l_name, loc_id, dept_id, concat(lower(email_id),'@gmail.com') as email_id, hire_date, salary  
                       from employee_dtls order by 1""")

SQL_qry.show(10)
print(f'Rows effected: {SQL_qry.count()}')