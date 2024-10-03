import pandas as pd
from elasticsearch import Elasticsearch, exceptions

es = Elasticsearch(
    ['http://localhost:9200'],
    basic_auth=('elastic', 'tRdmjs52knn3IU7EZAL6') 
)

def load_employee_data():
    try:
        data = pd.read_csv('C:/Users/Admin/OneDrive/Desktop/drive/Employee Sample Data 1.csv', encoding='latin1')
        
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col].fillna('', inplace=True)
            else:
                data[col].fillna(0, inplace=True)
        
        print("Employee data loaded successfully.")
        return data
    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit(1)
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        exit(1)

def create_collection(collection_name):
    collection_name = collection_name.lower()
    try:
        if not es.indices.exists(index=collection_name):
            es.indices.create(index=collection_name)
            print(f"Collection '{collection_name}' created.")
        else:
            print(f"Collection '{collection_name}' already exists.")
    except exceptions.AuthenticationException as e:
        print(f"Authentication error while creating collection '{collection_name}': {e}")
    except exceptions.RequestError as e:
        print(f"Request error while creating collection '{collection_name}': {e.info}")
    except Exception as e:
        print(f"An error occurred while creating collection '{collection_name}': {e}")

def index_data(collection_name, exclude_column):
    collection_name = collection_name.lower() 
    data = load_employee_data()
    for index, document in data.iterrows():
        doc_dict = document.where(pd.notnull(document), None).to_dict()
        if exclude_column in doc_dict:
            doc_dict.pop(exclude_column) 
        try:
            es.index(index=collection_name, id=document['Employee ID'], body=doc_dict)
            print(f"Indexed document {document['Employee ID']} into '{collection_name}'.")
        except exceptions.BadRequestError as e:
            print(f"Bad request error indexing document {document['Employee ID']} in '{collection_name}': {e.info}")
        except exceptions.AuthenticationException as e:
            print(f"Authentication error indexing document {document['Employee ID']} in '{collection_name}': {e}")
        except Exception as e:
            print(f"An error occurred while indexing document {document['Employee ID']} in '{collection_name}': {e}")

# Search records by a specific column
def search_by_column(collection_name, column_name, column_value):
    collection_name = collection_name.lower()
    try:
        response = es.search(index=collection_name, body={
            "query": {
                "match": {
                    column_name: column_value
                }
            }
        })
        print(f"\nSearch Results for {column_name} = '{column_value}' in '{collection_name}':")
        total_hits = response['hits']['total']['value']
        if total_hits > 0:
            print(f"Total Matches: {total_hits}")
            for hit in response['hits']['hits']:
                print(hit["_source"])
        else:
            print("No matching records found.")
    except exceptions.AuthenticationException as e:
        print(f"Authentication error during search in '{collection_name}': {e}")
    except exceptions.RequestError as e:
        print(f"Request error during search in '{collection_name}': {e.info}")
    except Exception as e:
        print(f"An error occurred during the search in '{collection_name}': {e}")

# Get employee count in the specified collection
def get_emp_count(collection_name):
    collection_name = collection_name.lower()
    try:
        count = es.count(index=collection_name)['count']
        print(f"Employee count in '{collection_name}': {count}")
        return count
    except exceptions.AuthenticationException as e:
        print(f"Authentication error while counting in '{collection_name}': {e}")
    except exceptions.RequestError as e:
        print(f"Request error while counting in '{collection_name}': {e.info}")
    except Exception as e:
        print(f"Error getting employee count in '{collection_name}': {e}")

# Delete an employee by ID
def delete_emp_by_id(collection_name, employee_id):
    collection_name = collection_name.lower()
    try:
        es.delete(index=collection_name, id=employee_id)
        print(f"Deleted employee with ID '{employee_id}' from '{collection_name}'.")
    except exceptions.NotFoundError:
        print(f"Employee with ID '{employee_id}' not found in '{collection_name}'.")
    except exceptions.AuthenticationException as e:
        print(f"Authentication error while deleting '{employee_id}' from '{collection_name}': {e}")
    except Exception as e:
        print(f"Error deleting employee with ID '{employee_id}' from '{collection_name}': {e}")

# Get department facet
def get_dep_facet(collection_name):
    collection_name = collection_name.lower()
    try:
        response = es.search(index=collection_name, body={
            "size": 0,
            "aggs": {
                "departments": {
                    "terms": {
                        "field": "Department.keyword",
                        "size": 10  
                    }
                }
            }
        })
        print(f"\nDepartment facet for '{collection_name}':")
        buckets = response['aggregations']['departments']['buckets']
        if buckets:
            for bucket in buckets:
                print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")
        else:
            print("No department data found.")
    except exceptions.AuthenticationException as e:
        print(f"Authentication error while getting facets in '{collection_name}': {e}")
    except exceptions.RequestError as e:
        print(f"Request error while getting facets in '{collection_name}': {e.info}")
    except Exception as e:
        print(f"Error getting department facet in '{collection_name}': {e}")

# Main function for user interaction
def main():
    v_name_collection = 'hash_kirithik'  
    v_phone_collection = 'hash_1234'     
    
    # Create collections
    create_collection(v_name_collection)
    create_collection(v_phone_collection)
    
    while True:
        print("\nChoose an operation:")
        print("1. Index Employee Data")
        print("2. Get Employee Count")
        print("3. Search Employee by ID")
        print("4. Delete Employee by ID")
        print("5. Get Department Facet")
        print("6. Exit")

        choice = input("Enter your choice (1-6): ").strip()

        if choice == '1':
            exclude_column = input("Enter the column name to exclude from indexing (or press Enter to exclude none): ").strip()
            if exclude_column:
                index_data(v_name_collection, exclude_column)
                index_data(v_phone_collection, exclude_column)  
            else:
                print("No column excluded. Indexing all columns.")
                index_data(v_name_collection, exclude_column=None)
                index_data(v_phone_collection, exclude_column=None)
        elif choice == '2':
            get_emp_count(v_name_collection)
        elif choice == '3':
            emp_id = input("Enter the Employee ID to search: ").strip()
            if emp_id:
                search_by_column(v_name_collection, 'Employee ID', emp_id)
            else:
                print("No Employee ID entered. Skipping search.")
        elif choice == '4':
            emp_id = input("Enter the Employee ID to delete: ").strip()
            if emp_id:
                delete_emp_by_id(v_name_collection, emp_id)
            else:
                print("No Employee ID entered. Skipping deletion.")
        elif choice == '5':
            get_dep_facet(v_name_collection)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please select a valid option (1-6).")

if __name__ == "__main__":
    main()
