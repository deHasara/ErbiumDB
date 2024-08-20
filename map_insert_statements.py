import json
from typing import List, Tuple, Dict, Any
import psycopg2

def flatten_composite(value: Any, type_name: str, custom_types: Dict[str, List[Tuple[str, str]]]) -> Tuple[List[Any], str]:
    if type_name not in custom_types:
        return [value], '%s'

    flat_values = []
    placeholders = []

    for attr_name, attr_type in custom_types[type_name]:
        if attr_type.endswith('[]'):
            base_type = attr_type[:-2]
            if isinstance(value[attr_name], list):
                sub_values, sub_placeholder = zip(*[flatten_composite(v, base_type, custom_types) for v in value[attr_name]])
                flat_values.extend([item for sublist in sub_values for item in sublist])
                placeholders.append(f"ARRAY[{', '.join(sub_placeholder)}]")
            else:
                sub_values, sub_placeholder = flatten_composite(value[attr_name], base_type, custom_types)
                flat_values.extend(sub_values)
                placeholders.append(f"ARRAY[{sub_placeholder}]")
        else:
            sub_values, sub_placeholder = flatten_composite(value[attr_name], attr_type, custom_types)
            flat_values.extend(sub_values)
            placeholders.append(sub_placeholder)

    return flat_values, f"ROW({', '.join(placeholders)})::{type_name}"

def generate_insert_statements(values, tables: List[Tuple[str, List[Tuple[str, str]]]], custom_types: Dict[str, List[Tuple[str, str]]]) -> List[str]:
    insert_statements = []

    for table_name, attributes in tables:
        # Check if we are dealing with a situation where there is an array attribute that has been normalized, then we will have to create multiple insert statements
        if len(attributes) == 2 and attributes[0][0] in values and attributes[1][0] in values and isinstance(values[attributes[1][0]], list) and not attributes[1][1].endswith('[]'):
            for item in values[attributes[1][0]]:
                values_copy = {attributes[0][0]: values[attributes[0][0]], attributes[1][0]: item}
                insert_statement = generate_insert_statement_for_one_table(table_name, attributes, values_copy, custom_types)
                if insert_statement:
                    insert_statements.append(insert_statement)
        else: 
            insert_statement = generate_insert_statement_for_one_table(table_name, attributes, values, custom_types)
            if insert_statement:
                insert_statements.append(insert_statement)

    return insert_statements



def generate_insert_statement_for_one_table(table_name: str, attributes: List[Tuple[str, str]], values: Dict[str, Any], custom_types: Dict[str, List[Tuple[str, str]]]) -> List[str]:
    temp_values = {}
    placeholders = {}
    print(attributes)
    for attr_name, attr_type, attr_unique_name in attributes:
        if attr_name in values:
            # Custom type without an array
            if attr_type in custom_types:  
                flat_values, placeholder = flatten_composite(values[attr_name], attr_type, custom_types)
                temp_values[attr_name] = flat_values
                placeholders[attr_name] = placeholder

            # Let's look at an array, however, we have to consider the case where the array is a custom type
            elif attr_type.endswith('[]'):  # Array attribute
                if attr_type[:-2] in custom_types:
                    base_type = attr_type[:-2]
                    sub_values = []
                    sub_placeholders = []
                    for item in values[attr_name]:
                        flat_values, placeholder = flatten_composite(item, base_type, custom_types)
                        sub_values.extend(flat_values)
                        sub_placeholders.append(f"ROW({', '.join(placeholder)})::{base_type}")
                    temp_values[attr_name] = sub_values
                    placeholders[attr_name] = f"ARRAY[{', '.join(sub_placeholders)}]"
                else: 
                    temp_values[attr_name] = values[attr_name]
                    placeholders[attr_name] = f"ARRAY[{', '.join(['%s'] * len(temp_values[attr_name]))}]"
            else:
                # Here we have a simple attribute, but the value could be a list
                temp_values[attr_name] = [values[attr_name]]
                placeholders[attr_name] = '%s'
        elif '__' in attr_name:  # Check for flattened composite attributes
            parent, child = attr_name.split('__', 1)
            if parent in values and isinstance(values[parent], dict):
                if child in values[parent]:
                    temp_values[attr_name] = [values[parent][child]]
                    placeholders[attr_name] = '%s'
    
    assert temp_values

    if temp_values:
        columns = ', '.join(temp_values.keys())
        placeholders_str = ', '.join(placeholders.values())
        flat_values = [item for sublist in temp_values.values() for item in (sublist if isinstance(sublist, list) else [sublist])]
        
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders_str})"
        return (table_name, attributes, insert_sql, tuple(flat_values))
    else: 
        return None

def format_sql_statement(sql: str, values: Tuple[Any, ...]) -> str:
    # Use psycopg2's mogrify function to properly format the SQL statement
    # We create a dummy connection that we won't actually use to connect
    dummy_conn = psycopg2.connect("dbname=erexpts1")
    dummy_cursor = dummy_conn.cursor()
    
    # mogrify returns bytes, so we decode it to a string
    formatted_sql = dummy_cursor.mogrify(sql, values).decode('utf-8')
    
    # Close the dummy connection
    dummy_conn.close()
    
    return formatted_sql

if __name__ == '__main__':  
    # Example usage
    tables = [
        ('rel0', [('person_id', 'INTEGER'), ('name', 'NAME_TYPE'), ('street', 'VARCHAR'), ('city', 'VARCHAR'), ('phone_numbers', 'VARCHAR[]')]),
        ('rel1', [('person_id', 'INTEGER'), ('phone_numbers', 'VARCHAR')]),
        ('rel2', [('course_id', 'INTEGER'), ('title', 'VARCHAR'), ('credits', 'VARCHAR')]),
        ('rel3', [('section_id', 'INTEGER'), ('course_id', 'INTEGER'), ('semester', 'VARCHAR'), ('year', 'INTEGER')]),
        ('rel4', [('person_id', 'INTEGER'), ('name__firstname', 'VARCHAR'), ('name__fakename__middlename', 'VARCHAR'), ('name__fakename__lastname', 'VARCHAR[]'), ('street', 'VARCHAR'), ('city', 'VARCHAR'), ('phone_numbers', 'VARCHAR[]')]),
        ('rel5', [('person_id', 'INTEGER'), ('lastname', 'VARCHAR')])
    ]

    custom_types = {
        'NAME_TYPE': [('firstname', 'VARCHAR'), ('fakename', 'LASTNAME_TYPE')],
        'LASTNAME_TYPE': [('middlename', 'VARCHAR'), ('lastname', 'VARCHAR[]')],
    }

    values = {
        'person_id': 1,
        'name': {'firstname': 'fname', 'fakename': {'middlename': 'mname', 'lastname': ['lname1', 'lname2']}},
        'street': 'st',
        'city': 'c',
        'phone_numbers': ['101', '102']
    }

    insert_statements = generate_insert_statements(tables, values, custom_types)

    for tablename, attributes, statement, values in insert_statements:
        print(f"SQL: {statement}")
        print(f"Values: {values}")
        print()

    for tablename, attributes, statement, values in insert_statements:
        print(f"=== For table: {tablename, attributes}")
        print(f"Formatted SQL: {format_sql_statement(statement, values)}\n")
