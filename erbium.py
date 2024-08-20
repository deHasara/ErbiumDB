import argparse
import json
import psycopg2
from psycopg2 import sql
import cmd

import logging
import readline

from sql_parser import parse
from sql_analyzer import parse_and_analyze
from er_graph import Graph, deserialize_graph, serialize_graph, Node, Edge, NodeType, EdgeType
import json

from construct_create_statements import create_table_statements, figure_out_mappings
from map_insert_statements import generate_insert_statements, format_sql_statement
from map_select_queries import generate_sql_query

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class ERShell(cmd.Cmd):
    prompt = 'ersh> '
    intro = 'Welcome to ErbiumDB. Type help or ? to list commands.\n'

    def __init__(self, db_name, tables, types, graph):
        super().__init__()
        self.db_name = db_name
        self.tables = tables
        self.types = types
        self.graph = graph

    def default(self, arg):
        if "select" == arg[:6]:
            self.do_query(arg)
        else:
            return self.do_exit(arg)

    def do_exit(self, arg):
        """Exit the shell"""
        print("Exiting...")
        return True
        
    def do_query(self, arg):
        """Execute a query"""
        print(arg)
        run_query(self.db_name, arg, self.tables, self.types, self.graph)

def load_data(db_name):
    conn = psycopg2.connect(f"dbname={db_name}")
    cursor = conn.cursor()

    # Query the erdb_objects table for tables, types, and graph
    cursor.execute("SELECT name, data FROM erdb_objects WHERE name IN ('tables', 'types', 'graph')")
    rows = cursor.fetchall()

    # Deserialize the JSON data
    for row in rows:
        name, data = row
        if name == "tables": tables = data
        elif name == "types": types = data
        elif name == "graph": graph = deserialize_graph(json.dumps(data))
        else:   
            logging.debug(f"Unknown object: {name}")
            assert False

    cursor.close()
    conn.close()

    return tables, types, graph

def run_query(db_name, query, tables, types, graph):
    result = parse_and_analyze(query)
    print(result)
    sql = generate_sql_query(tables, graph.get_node_by_name(result['table_name']), graph)

    print("---- Running query on database:")
    print(sql)
    print("-------")

    # Run the query and output the results one by one
    conn = psycopg2.connect(f"dbname={db_name}")
    cursor = conn.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)
    return

def create_database_if_not_exists(db_name):
    assert db_name != "postgres", "Cannot use the default PostgreSQL database"

    # Connect to the PostgreSQL server
    conn = psycopg2.connect("dbname=postgres")
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if the database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cursor.fetchone()
    
    if exists:
        logging.debug(f"Database {db_name} already exists.. deleting and recreating")

        # Connect to the existing database
        conn.close()
        conn = psycopg2.connect(f"dbname={db_name}")
        cursor = conn.cursor()

        # Delete all tables from the database
        cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """)

        tables = cursor.fetchall()

        for table in tables:
            logging.debug("Dropping table: %s", table[0])
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")

        conn.commit()
    else:
        logging.debug(f"Database {db_name} does not exist.. creating")
        # Create the database
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    
        # Close the connection
    cursor.close()
    conn.close()

def init_database(db_name, load_file):
    create_database_if_not_exists(db_name)

    conn = psycopg2.connect(f"dbname={db_name}")
    cursor = conn.cursor()

    # Table to hold the metadata as JSON -- there really should only be one row in this
    cursor.execute("CREATE TABLE erdb_objects (id serial primary key, name text, data JSONB)")

    graph = Graph()

    with open(load_file, "r") as f:
        data = json.load(f)
        create_entity_statements = data["create_entity_statements"]
        create_relationship_statements = data["create_relationship_statements"]
        connected_subgraphs = data[data["use_connected_subgraph"]]
    
    for statement in create_entity_statements:
        result = parse_and_analyze(statement)
        graph.add_entity(result)
        logging.debug(f"Parsed: {statement}")
        logging.debug(f"Result: {result}")

    for statement in create_relationship_statements:
        result = parse_and_analyze(statement)
        graph.add_relationship(result)
        logging.debug(f"Parsed: {statement}")
        logging.debug(f"Result: {result}")

    # Process connected_subgraphs
    for subgraph in connected_subgraphs:
        logging.debug(f"Connected Subgraph: {subgraph}")

    tables, types = create_table_statements(graph, connected_subgraphs)

    # Create the types
    for x in types:
        t = types[x]
        #sql_statement = f"CREATE TYPE {table[0]}"
        #sql_statement += " (" + ", ".join([attr[0] + " " + attr[1] for attr in table[1]]) + ")"
        #logging.debug(sql_statement)
        sql_statement = f"CREATE TYPE {x} AS"
        sql_statement += " (" + ", ".join([attr[0] + " " + attr[1] for attr in t]) + ")"
        logging.debug(sql_statement)
        cursor.execute(sql_statement)

    for t in tables:
        sql_statement = f"CREATE TABLE {t[0]}"
        sql_statement += " (" + ", ".join([attr[0] + " " + attr[1] for attr in t[1]]) + ")"
        logging.debug(sql_statement)
        cursor.execute(sql_statement)

    figure_out_mappings(graph, connected_subgraphs, tables)

    # Serialize the objects to JSON
    tables_json = json.dumps(tables)
    types_json = json.dumps(types)
    graph_json = serialize_graph(graph)

    print(json.dumps(json.loads(graph_json), indent=4))

    # Insert the serialized data into the database
    cursor.execute("INSERT INTO erdb_objects (name, data) VALUES (%s, %s)", ("tables", tables_json))
    cursor.execute("INSERT INTO erdb_objects (name, data) VALUES (%s, %s)", ("types", types_json))
    cursor.execute("INSERT INTO erdb_objects (name, data) VALUES (%s, %s)", ("graph", graph_json))

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def match_to_schema_helper(values, attributes_with_structure):
    ret = {}
    if not isinstance(values, list):
        assert not isinstance(attributes_with_structure, list), "Expected a scalar"
        return {attributes_with_structure["attr_name"]: values}

    assert isinstance(attributes_with_structure, list), "Expected a list"   

    for x, y in zip(values, attributes_with_structure):
        if y.get("is_multivalued", False):
            assert isinstance(x, list), f"Expected a list for {y.attr_name}"
            y["is_multivalued"] = False
            arr = [match_to_schema_helper(entry, y) for entry in x] # needed to handle arrays of composite types
            ret[y["attr_name"]] = [entry[y["attr_name"]] for entry in arr]
            y["is_multivalued"] = True
        elif y["attr_type"] == 'COMPOSITE':
            ret[y["attr_name"]] = match_to_schema_helper(list(x), y["sub_attributes"])
        else: 
            if y["attr_type"] == 'INT':
                ret[y["attr_name"]] = int(x)
            else: 
                ret[y["attr_name"]] = x
    return ret

def match_to_schema(table_name, values, entity):
    # find the entity in the graph
    attributes_with_structure = entity.attributes_with_structure
    return match_to_schema_helper(values, attributes_with_structure)
            

def insert_data(db_name, load_file):
    with open(load_file, "r") as f:
        data = json.load(f)
        insert_statements = data["insert_statements"]

    tables, types, graph = load_data(db_name)

    conn = psycopg2.connect(f"dbname={db_name}")
    cursor = conn.cursor()

    # Insert data
    for insert_statement in insert_statements:
        logging.debug(f"Insert Statement: {insert_statement}")
        parsed = parse_and_analyze(insert_statement)
        entity = [node for node in graph.nodes if node.name.lower() == parsed["table_name"].lower()][0]
        values_as_dict = match_to_schema(parsed["table_name"], parsed["values"], entity)
        relevant_tables = [table for table in tables if table[0] in entity.tables]
        insert_data = generate_insert_statements(values_as_dict, relevant_tables, types)
        for _, _, statement, values in insert_data:
            formatted_statement = format_sql_statement(statement, values)
            cursor.execute(formatted_statement)
            conn.commit()

    cursor.close()
    conn.close()



def main():
    parser = argparse.ArgumentParser(description="ER Shell")
    parser.add_argument("command", choices=["init", "shell", "insert"], help="Command to execute")
    parser.add_argument("db_name", help="Database name")
    parser.add_argument("load_file", nargs="?", help="CREATE file for initialization as JSON")

    args = parser.parse_args()

    if args.command == "init" or args.command == "insert":
        if not args.load_file:
            print("A file with create table statements is required for initialization")
            return
        if args.command == "init":
            init_database(args.db_name, args.load_file)
        else: 
            insert_data(args.db_name, args.load_file)
        print(f"Database {args.db_name} initialized with data from {args.load_file}")
    elif args.command == "shell":
        tables, types, graph = load_data(args.db_name)
        shell = ERShell(args.db_name, tables, types, graph)
        shell.cmdloop()
        #queries = ["select * from (instructor join section on teaches) join course on course_id", "select * from person", "select * from instructor"]
        #run_query(args.db_name, queries[2], tables, types, graph)

if __name__ == "__main__":
    main()
