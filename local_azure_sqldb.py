import json
import pyodbc
server = 'sqlserver45.database.windows.net'
database = 'ky2910'
username = 'ky2910'
password = 'XXXxxxx'
driver= '{SQL Server}'
conn = pyodbc.connect('Driver='+driver+';Server=tcp:'+server+',1433;Database='+database+';Uid='+username+';PWD='+ password)
cursor = conn.cursor()
# print(json.dumps({"Name": "test", "Size": "331 bytes", "Item type": "JSON File", "Date modified": "22-08-2022 15:46", "Date created": "22-08-2022 12:21"}))
cursor.execute("create table test_task(id int,name varchar(10))")
cursor.commit()
conn.close()
