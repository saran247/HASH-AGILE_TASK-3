# HASH-AGILE_TASK-3
Function Definitions
createCollection(p_collection_name)
Using Any of the programming language implement below functions
indexData(p_collection_name, p_exclude_column):
Index the given employee data into the specified collection, excluding the column provided in p_exclude_column.
searchByColumn(p_collection_name, p_column_name, p_column_value):
Search within the specified collection for records where the column p_column_name matches the value p_column_value.
getEmpCount(p_collection_name)
delEmpById(p_collection_name, p_employee_id)
•  getDepFacet(p_collection_name):
Retrieve the count of employees grouped by department from the specified collection.

Function Executions

Var v_nameCollection = ‘Hash_<Your Name>’
Once the functions are implemented, execute the functions in the given order with the parameters mentioned
Var v_phoneCollection =’Hash_<Your Phone last four digits’
createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection,’Department’)
indexData(v_ phoneCollection, ‘Gender’)
delEmpById (v_ nameCollection ,‘E02003’)
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection,’Department’,’IT’)
searchByColumn(v_nameCollection,’Gender’ ,’Male’)
searchByColumn(v_ phoneCollection,’Department’,’IT’)
getDepFacet(v_ nameCollection)
getDepFacet(v_ phoneCollection)
