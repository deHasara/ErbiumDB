from er_graph import NodeType, EdgeType, Graph, Edge, Node
import json
from typing import List, Dict, Any, Tuple

## We could use a different modifier to create the internal primary keys 
## For now, we will assume that each main entity has an "_id" attribute that we can use for this purpose
INTERNAL_MODIFIER = ""


##############################################################################################################
### This module will generate the create table statements for given ER graph and a set of connected subgraphs.
##############################################################################################################

def get_attribute_type(attr_type: str) -> str:
    if attr_type == 'INT':
        return 'INTEGER'
    elif attr_type == 'VARCHAR':
        return 'VARCHAR(255)'
    elif attr_type == 'COMPOSITE':
        return 'JSONB'
    else:
        return 'TEXT'


# Construct a CREATE TYPE statement for composite type that needs to be created. This is done recursively so that we might create composite types that contain other composite types.
#
# The return value here will be a list for each composite type
def create_composite_type(graph: Graph, node: Node, created_types_names: set, created_types: list) -> Tuple[List[str], str]:
    type_name = f"{node.unique_name.replace('.', '_')}_type"
    if type_name in created_types:
        return type_name

    sub_columns = []

    for sub_node in node.children:
        if sub_node.attr_type == 'COMPOSITE':
            sub_type_name = create_composite_type(graph, sub_node, created_types_names, created_types)
            sub_columns.append((sub_node.unique_name.split('.')[-1], sub_type_name))
        else:
            sub_columns.append((sub_node.unique_name.split('.')[-1], get_attribute_type(sub_node.attr_type)))

    created_types_names.add(type_name)
    created_types[type_name] = sub_columns

    return type_name

# Subclassses take some effort to get right
# For each subclass, we need to know whether it is going to be a separate table, and also whether it is broken off from the parent
# We tell this based on whether there exists a connected subgraph that includes the subclass, its parent entity, and at least one of its attributes, but no relationships
#
# The reasoning is similar to understand if a weak entity is by itself or not, so we will combine the two
def helper_figure_out_subclass_or_weak_entity_status(graph, connected_subgraphs):
    for node in graph.nodes:
        if node.is_entity() and (node.is_subclass or node.is_weak_entity):
            parent_node = node.parent_entity
            node_attribute = graph.get_attributes(node)[0] # For now, we will assume that there is at least one attribute

            # check if there is a subgraph that contains the subclass/weak entity, its parent entity, and at least one of its attributes
            found_subgraph = None
            for subgraph in connected_subgraphs:
                if node.unique_name in subgraph and parent_node.unique_name in subgraph and node_attribute.unique_name in subgraph:
                    found_subgraph = subgraph
                    break

            # there must be a subgraph if the node is a weak entity
            assert node.is_subclass or found_subgraph

            # check if the found_subbgraph contains any relationships
            # This only matters for subclasses
            if found_subgraph and node.is_subclass:
                has_relationship = False
                for unique_name in found_subgraph:
                    n_node = graph.get_node_by_name(unique_name)
                    if n_node.is_relationship():
                        has_relationship = True
                        break

            # finally we need to know if at least one of the parent's attributes is in the subgraph
            if found_subgraph:
                parent_attributes = graph.get_attributes(parent_node)
                parent_attribute_in_subgraph = False
                for parent_attribute in parent_attributes:
                    if parent_attribute.unique_name in found_subgraph:
                        parent_attribute_in_subgraph = True

            if node.is_subclass:
                # This means that the subclass has a table on its own with no connection to the parent entity
                node.all_by_itself = not found_subgraph 

                # This means that the subclass is entirely inside the parent entity table, with NULLs used for the subclass attributes as needed
                node.contained_in_parent = found_subgraph and not has_relationship and parent_attribute_in_subgraph

                # This means there is a separate table for the subclass attributes, with a foreign key to the parent entity
                node.partially_by_itself = found_subgraph and not has_relationship and not parent_attribute_in_subgraph

                assert node.all_by_itself or node.contained_in_parent or node.partially_by_itself
            else: 
                node.all_by_itself = not parent_attribute_in_subgraph

# Next, we will consider composite types, and try to keep track of whether they are flattened or not

def create_table_statements(graph: Graph, connected_subgraphs: List[List[str]]):
    tables_to_be_created = []
    created_types_names = set()
    created_types = {}

    # For each subclass, figure out whether it is separated out, or partially or totally contained within its parent
    helper_figure_out_subclass_or_weak_entity_status(graph, connected_subgraphs)

    for i, subgraph in enumerate(connected_subgraphs):
        table_name = f"rel{i}"
        columns = []

        table_attributes = []

        subgraph_copy = subgraph.copy()
        for n in subgraph_copy:
            x = graph.get_node_by_name(n)
            assert x

        # check if the subgraph has repeated entries
        has_repeated_entities = False
        if len(set(subgraph)) != len(subgraph):
            has_repeated_entities = True

            # there must be a relationship involved -- decide what should be the names of the two referring attributes
            relationship_node = None
            for unique_name in subgraph:
                node = graph.get_node_by_name(unique_name)
                if node.is_relationship():
                    relationship_node = node
                    break
            assert relationship_node

            if relationship_node.recursive_relationship_roles:
                role1, role2 = relationship_node.recursive_relationship_roles
                # TODO the code wouldn't handle a recursive relationship that involves a weak entity
                table_attributes.append((f"{role1[:-3]}_{INTERNAL_MODIFIER}id", "INTEGER", f"{role1[:-3]}_{INTERNAL_MODIFIER}id"))
                table_attributes.append((f"{role2[:-3]}_{INTERNAL_MODIFIER}id", "INTEGER", f"{role2[:-3]}_{INTERNAL_MODIFIER}id"))
            else: 
                # This should be the situation where the relationship is between two entities that are subclasses of the same entity
                # Let's get the entity names of the two endpoints of the relationship and use those as the column names
                e1, e2 = None, None
                for n in graph.get_neighbors(relationship_node):
                    if n.is_entity():
                        if not e1:
                            e1 = n
                        else:
                            e2 = n
                            break
                assert e1 and e2    
                table_attributes.append((f"{e1.unique_name.split('.')[-1]}_{INTERNAL_MODIFIER}id", "INTEGER", f"{e1.unique_name.split('.')[-1]}_{INTERNAL_MODIFIER}id"))
                table_attributes.append((f"{e2.unique_name.split('.')[-1]}_{INTERNAL_MODIFIER}id", "INTEGER", f"{e1.unique_name.split('.')[-1]}_{INTERNAL_MODIFIER}id"))

        for unique_name in subgraph:
            node = graph.get_node_by_name(unique_name)
            if node:
                if node.is_entity(): ## For now, we will assume that we create a special internal ID for this purpose
                    if has_repeated_entities:
                        continue
                    if not node.is_subclass:
                        table_attributes.insert(0, (f"{unique_name}_{INTERNAL_MODIFIER}id", "INTEGER", f"{unique_name}_{INTERNAL_MODIFIER}id"))
                    else: 
                        if node.contained_in_parent:
                            pass
                        elif node.all_by_itself:
                            table_attributes.insert(0, (f"{unique_name}_{INTERNAL_MODIFIER}id", "INTEGER", f"{unique_name}_{INTERNAL_MODIFIER}id"))
                        elif node.partially_by_itself:
                            pass # we will have the parent entity special internal key
                            #columns.append(f"{unique_name}_{INTERNAL_MODIFIER}id INTEGER REFERENCES {node.parent_entity.lower()}_{INTERNAL_MODIFIER}id")
                elif node.is_relationship():
                    pass
                elif node.is_attribute():
                    # If the attribute has a parent_attribute, that means we are "flattening" the composite type
                    # But since we already have the ".", we can simply replace those with "__"
                    attribute_name = unique_name.split('.', 1)[-1].replace('.', '__')

                    if unique_name[-3:] == '_id':
                        continue
                    elif node.is_composite:
                        # We should add in asserts to confirm that none of its children are in any subgraph
                        type_name = create_composite_type(graph, node, created_types_names, created_types)
                        table_attributes.append( (attribute_name, type_name, unique_name) )
                    elif node.is_multivalued:
                            # This is going to depend on whether this is the only attribute in this connected subgraph
                            if len([n for n in subgraph_copy if graph.get_node_by_name(n).is_attribute()]) == 1:
                                #columns.append(f"{unique_name.split('.')[-1]} {get_attribute_type(node.attr_type)}")
                                table_attributes.append((f"{attribute_name}", get_attribute_type(node.attr_type), unique_name))
                            else: 
                                #columns.append(f"{unique_name.split('.')[-1]} {get_attribute_type(node.attr_type)}[]")
                                table_attributes.append((f"{attribute_name}", str(get_attribute_type(node.attr_type)) + "[]", unique_name))
                    else: 
                        #columns.append(f"{unique_name.split('.')[-1]} {get_attribute_type(node.attr_type)}")
                        table_attributes.append((f"{attribute_name}", get_attribute_type(node.attr_type), unique_name))
                else:
                    assert False

        tables_to_be_created.append((table_name, table_attributes))

        #create_statements.append(f"CREATE TABLE {table_name} (")
        #create_statements.append(",\n".join(f"    {column}" for column in columns))
        #create_statements.append(");")

    return tables_to_be_created, created_types


# Given the tables and the types, let's figure out exactly which tables contain data for each entity
# and relationship
def figure_out_mappings(graph, connected_subgraphs, created_tables):
    for node in graph.nodes:
        if node.is_entity():
            node.tables = set()
            # for a regular entity, we look for occurences of its attributes in the connected subgraphs
            for attribute in graph.get_attributes(node):
                for i in range(len(connected_subgraphs)):
                    table_name, attributes = created_tables[i]
                    cg = connected_subgraphs[i]
                    if node.unique_name in cg and attribute.unique_name in [a[2] for a in attributes]:
                        node.tables.add(table_name)
                        break
            
            if node.is_subclass and not node.all_by_itself:
                node.tables |= node.parent_entity.tables            

            print(node.unique_name, node.tables)

        elif node.is_relationship():
            # Look for the connected subgraph containing that relationship. There can be only one for now
            for i in range(len(connected_subgraphs)):
                cg = connected_subgraphs[i]
                if node.unique_name in cg:
                    node.tables = {created_tables[i][0]}
                    break

            print(node.unique_name, node.tables)


