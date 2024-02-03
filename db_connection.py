from settings import DB_HOST, DB_DATABASE, DB_USERNAME, DB_PASSWORD
# from utils.uuid_generation import generate_uuid, generate_uuid_from_string

__conn = None


def connect_db(db_type: str):
    '''
    If connection exists, close existing connection. Then, connect to new connection.
    '''

    # Declare conn as global variable
    global __conn

    try:

        if __conn is not None:
            __conn.close()

        # List of alloweed db_type
        db_type_list = ['mysql', 'mssql']

        if db_type.strip().lower() == 'mysql':
            import mysql.connector
            __conn = mysql.connector.connect(host=DB_HOST, database=DB_DATABASE, user=DB_USERNAME, password=DB_PASSWORD)
            response = f'Connected {db_type} to host={DB_HOST}, database={DB_DATABASE}'
            return __conn, response
        
        elif db_type.lstrip().lower() == 'mssql':
            import pyodbc
            __conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_HOST};DATABASE={DB_DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}') 
            response = f'Connected {db_type} to host={DB_HOST}, database={DB_DATABASE}'
            return __conn, response

        else:
            raise NotImplementedError (f'Provided db_type is not in {db_type_list} !')

    except Exception as e:
        response = f'Failed to connect: {str(e)}'
        return None, response


# def generate_insert_statement(table: str, data_dict: dict, uuid_col_list: list, generate_uuid_col_name: str,
#                               primary_col_list: list, password_col_list: list):
#     '''
#     Usage:
#     1) Define table to be inserted
#     2) data_dict keys should match the table column name, and primary_UUID column should be excluded here
#     3) All uuid-type column MUST be defined in uuid_col_list for UUID_TO_BIN conversion!
#     4) If a primary UUID needs to be generated, define primary_col_list used to generate the primary UUID
#         it will be named as generate_uuid_col_name!
#     5) Define all password column in password_col_list for string to UUID conversion

#     6)Return insert statement like :
#     INSERT INTO schema.table (person_uuid, name, gender_uuid, height)
#     VALUES (UUID_TO_BIN(%s), %s, UUID_TO_BIN(%s), %s);
#     ('554260d7887657ac9233f300c1c2cda3', 'jiajunlee', 'ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2', 170)
#     '''

#     # Hash password string into UUID form
#     if len(password_col_list) > 0:
#         for password_col in password_col_list:
#             data_dict[password_col] = generate_uuid_from_string(data_dict[password_col])

#     # Split data_dict by key list and value list
#     column_list = list(data_dict.keys())  # ['name', 'gender_uuid', 'height']
#     value_list = list(data_dict.values())  # ['jiajunlee', 'bc0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2', 170]

#     # column_tuple =  'person_uuid, name, gender_uuid, height'
#     if len(uuid_col_list) > 0 and len(generate_uuid_col_name) > 0:
#         column_list.insert(0, generate_uuid_col_name)  # ['person_uuid', 'name', 'gender_uuid', 'height']
#     column_tuple = ', '.join(column_list)

#     # value_tuple = 'UUID_TO_BIN(%s), %s, UUID_TO_BIN(%s), %s'
#     value_tuple_list = ['%s'] * len(column_list)
#     if len(uuid_col_list) > 0:
#         for uuid_col in uuid_col_list:
#             uuid_col_index = column_list.index(uuid_col)
#             value_tuple_list[uuid_col_index] = 'UUID_TO_BIN(%s)'
#     value_tuple = ', '.join(value_tuple_list)

#     # data_tuple = ('jiajunlee','ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2',170)
#     if len(uuid_col_list) > 0 and len(generate_uuid_col_name) > 0:
#         # primary_value_list = ['jiajunlee', 'bc0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2']
#         primary_value_list = [data_dict[primary_col] for primary_col in primary_col_list]
#         uuid = generate_uuid(primary_value_list)
#         data_tuple = tuple(uuid.replace('-', '').split(' ')) + tuple(value_list)
#     else:
#         uuid = None
#         data_tuple = tuple(value_list)

#     # Generating query statement
#     query = f'''
#         INSERT INTO {table} ({column_tuple})
#         VALUES ({value_tuple});
#     '''

#     return query, data_tuple, uuid


# def generate_update_statement(table: str, data_dict: dict, uuid_col_list: list, password_col_list: list,
#                               condition_key: str, condition_value: str):
#     '''
#     Usage:
#     1) Define table to be updated
#     2) data_dict keys should match the table column name, define what to be updated
#     3) All uuid-type column MUST be defined in uuid_col_list for UUID_TO_BIN conversion!
#     4) Define all password column in password_col_list for string to UUID
#     5) Define condition key and condition value to which row to be updated

#     6)Return update statement like :
#         UPDATE schema.table
#         SET gender_uuid = UUID_TO_BIN(%s), height = %s
#         WHERE person_id = 'bc0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2';
#          ('ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2', 170)
#     '''

#     # Hash password string into UUID form
#     if len(password_col_list) > 0:
#         for password_col in password_col_list:
#             data_dict[password_col] = generate_uuid_from_string(data_dict[password_col])

#     # Split data_dict by key list and value list
#     column_list = list(data_dict.keys())  # ['gender_uuid', 'height']
#     value_list = list(data_dict.values())  # ['bc0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2', 170]
#     uuid = condition_value

#     # set_string = SET gender_uuid = UUID_TO_BIN(%s), height = %s
#     set_string_list = []
#     for column in column_list:
#         if len(uuid_col_list) > 0:
#             if column in uuid_col_list:
#                 set_string_list.append(f'{column} = UUID_TO_BIN(%s)')
#                 continue
#         set_string_list.append(f'{column} = %s')
#     set_string = ', '.join(set_string_list)

#     # data_tuple = ('ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2',170)
#     data_tuple = tuple(value_list)

#     # Generating query statement
#     query = f'''
#         UPDATE {table}
#         SET {set_string}
#         WHERE BIN_TO_UUID({condition_key}) = '{uuid}';
#     '''

#     return query, data_tuple, uuid


def select_query(cursor, query, data=None):
    '''Parse cursor, query and return select query response'''
    if data is None:
        cursor.execute(query)
    else:
        cursor.execute(query, data)
    fetched = cursor.fetchall()
    columns = cursor.description
    response = []
    if cursor is not None:
        for result in fetched:
            row_dict = {}
            for i, column in enumerate(result):
                row_dict[columns[i][0]] = column
            response.append(row_dict)

    return response


if __name__ == '__main__':
    connection, response = connect_db(db_type='mssql')
    try:

        cursor = connection.cursor()
        query = '''
                    SELECT * FROM [localdb].[dbo].[table1]
                '''
        print(select_query(cursor, query))
    except Exception as e:
        print(str(e))

    # insertQuery, insertData, insertUUID = generate_insert_statement(
    #     'schema.table',
    #     {
    #         'name': 'jiajunlee',
    #         'gender_uuid': 'ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2',
    #         'height': 170
    #     },
    #     ['person_uuid', 'gender_uuid'],
    #     'person_uuid',
    #     ['name', 'gender_uuid'],
    #     []
    # )
    # print(insertQuery, insertData, insertUUID)

    # updateQuery, updateData, updateUUID = generate_update_statement(
    #     'schema.table',
    #     {
    #         'gender_uuid': 'ab0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2',
    #         'height': 170
    #     },
    #     ['person_uuid', 'gender_uuid'],
    #     [],
    #     'person_id',
    #     'bc0c0bbc-fcbe-5d85-8a5c-5f603aecbeb2'
    # )
    # print(updateQuery, updateData, updateUUID)

    if connection is not None:
        connection.close()
        print('Connection closed')