from enum import Enum
import json
from typing import Dict, List, Any, Optional
import pprint
from sql_analyzer import EntityType
import copy


#######################################
########### Nodes and Edges
#######################################
class NodeType(Enum):
    ENTITY = 1
    RELATIONSHIP = 2
    ATTRIBUTE = 3

class EdgeType(Enum):
    ENTITY_ATTRIBUTE = 1
    ATTRIBUTE_ATTRIBUTE = 2
    ENTITY_RELATIONSHIP = 3
    ENTITY_ENTITY = 4

class Node:
    def __init__(self, name: str, unique_name: str = None):
        self.name = name
        self.unique_name = unique_name.lower() if unique_name else self.name.lower()
        self.type = None

    def is_attribute(self):
        return self.type == NodeType.ATTRIBUTE
    def is_entity(self):
        return self.type == NodeType.ENTITY
    def is_relationship(self):
        return self.type == NodeType.RELATIONSHIP

class Entity(Node):
    def __init__(self, name: str, unique_name: str = None):
        super().__init__(name, unique_name)
        self.is_subclass = False
        self.is_weak_entity = False
        self.parent_entity = None
        self.type = NodeType.ENTITY
        self.entity_dict = None

        # we will keep the attributes explicitly
        self.attributes = []

class Relationship(Node):
    def __init__(self, name: str, unique_name: str = None):
        super().__init__(name, unique_name)
        self.type = NodeType.RELATIONSHIP
        self.recursive_relationship_roles = None

        # the entities that it connects to
        self.entity1 = None
        self.entity2 = None

        # we will keep the attributes explicitly
        self.attributes = []

class Attribute(Node):
    def __init__(self, name: str, unique_name: str, attr_type: str):
        super().__init__(name, unique_name)
        self.attr_type = attr_type
        self.is_multivalued = False
        self.is_composite = False
        self.entity = None
        self.parent_attribute = None
        self.type = NodeType.ATTRIBUTE

class Edge:
    def __init__(self, edge_type: EdgeType, source: Node, target: Node, properties: Dict[str, Any] = None):
        self.edge_type = edge_type
        self.source = source
        self.target = target

#######################################
########### Graph
#######################################
class Graph:
    def __init__(self):
        self.nodes: List[Node] = []
        self.edges: List[Edge] = []

    def add_node(self, node: Node):
        self.nodes.append(node)
        return node

    def add_edge(self, edge: Edge):
        self.edges.append(edge)
        return edge

    def get_node_by_name(self, unique_name: str):
        for node in self.nodes:
            if node.unique_name == unique_name.lower():
                assert node
                return node
        assert f"Node with unique name {unique_name} not found"

    def get_edges_by_node(self, node: Node) -> List[Edge]:
        return [edge for edge in self.edges if edge.source == node or edge.target == node]

    def get_neighbors(self, node: Node) -> List[Node]:
        neighbors = []
        for edge in self.get_edges_by_node(node):
            if edge.source == node:
                neighbors.append(edge.target)
            else:
                neighbors.append(edge.source)
        return neighbors

    # For an entity node, get all its attributes
    def get_attributes(self, node: Node) -> List[Node]:
        assert node.type == NodeType.ENTITY and node.attributes
        return node.attributes

    def add_entity(self, entity_dict):
        n = self.add_node(Entity(entity_dict['table_name']))

        n.entity_dict = entity_dict
        n.is_subclass = entity_dict['entity_type'] == EntityType.SUBCLASS
        n.is_weak_entity = entity_dict['entity_type'] == EntityType.WEAK    
        n.attributes = []

        for attr in entity_dict['attributes']:
            # Takes care of recursive composite attributes
            self.add_attribute(attr, entity_dict['table_name'], entity = n, parent_attribute = None) 

        if n.is_subclass or n.is_weak_entity:
            n.parent_entity = self.get_node_by_name(entity_dict['parent_entity'])
            assert n.parent_entity
            self.add_edge(Edge(EdgeType.ENTITY_ENTITY, n, n.parent_entity))

    def add_attribute(self, attr, parent_unique_name, entity, parent_attribute):
        unique_name = (parent_unique_name + "." + attr['attr_name']).lower()

        this_node = self.add_node(Attribute(attr['attr_name'], unique_name, attr_type = attr['attr_type']))
        this_node.is_composite = (attr['attr_type'].upper() == 'COMPOSITE')
        this_node.is_multivalued = attr.get('is_multivalued', False)
        if parent_attribute:
            this_node.parent_attribute = parent_attribute
            parent_attribute.children.append(this_node)
        this_node.entity = entity

        entity.attributes.append(this_node)

        if this_node.is_composite:
            this_node.children = []
            for sub_attr in attr['sub_attributes']:
                 self.add_attribute(sub_attr, unique_name, entity, this_node)

    def add_relationship(self, rel_dict):
        e1 = self.get_node_by_name(rel_dict['entity1']['name'])
        e2 = self.get_node_by_name(rel_dict['entity2']['name'])
        assert e1 and e2

        n = self.add_node(Relationship(rel_dict['table_name']))
        n.entity1 = e1
        n.entity2 = e2

        n.rel_dict = rel_dict

        for attr in rel_dict['attributes']:
            self.add_attribute(attr, rel_dict['table_name'], entity = n, parent_attribute = None)

        self.add_edge(Edge(EdgeType.ENTITY_RELATIONSHIP, n, e1))
        self.add_edge(Edge(EdgeType.ENTITY_RELATIONSHIP, n, e2))

        if rel_dict['entity1']['role']:
            n.recursive_relationship_roles = (rel_dict['entity1']['role'], rel_dict['entity2']['role'])
        
#####################################################
########### Graph Serialization and Deserialization
#####################################################
import json
from typing import Dict

class GraphEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Node):
            node_data = {
                "type": "node",
                "name": obj.name,
                "unique_name": obj.unique_name,
            }
            if isinstance(obj, Entity):
                # we need the primary key from the parent entity 
                if obj.is_weak_entity:
                    obj.attributes_with_structure = copy.deepcopy(obj.entity_dict['attributes'])
                    obj.attributes_with_structure.insert(0, copy.deepcopy(obj.parent_entity.entity_dict['attributes'][0]))
                elif obj.is_subclass:
                    obj.attributes_with_structure = copy.deepcopy(obj.parent_entity.attributes_with_structure) + copy.deepcopy(obj.entity_dict['attributes'])

                    # We may need to change the ID attribute
                    if obj.all_by_itself:
                        obj.attributes_with_structure[0]['attr_name'] = f"{obj.unique_name}_id"

                else: 
                    obj.attributes_with_structure = copy.deepcopy(obj.entity_dict['attributes'])

                node_data.update({
                    "node_type": "ENTITY",
                    "is_subclass": obj.is_subclass,
                    "is_weak_entity": obj.is_weak_entity,
                    "parent_entity": obj.parent_entity.unique_name if obj.parent_entity else None,
                    "attributes": [attr.unique_name for attr in obj.attributes],
                    "tables": list(obj.tables),
                    "attributes_with_structure": obj.attributes_with_structure
                })
                if obj.is_subclass:
                    node_data.update({
                        "partially_by_itself": obj.partially_by_itself,
                        "all_by_itself": obj.all_by_itself,
                        "contained_in_parent": obj.contained_in_parent
                    })
            elif isinstance(obj, Relationship):
                obj.attributes_with_structure = copy.deepcopy(obj.rel_dict['attributes'])

                # We need to add the attributes of the entities as well
                # The subclass attribute renamining should be taken care of because we use attributes_with_structure 
                if obj.entity2.is_weak_entity:
                    obj.attributes_with_structure.insert(0, copy.deepcopy(obj.entity2.attributes_with_structure[1]))
                obj.attributes_with_structure.insert(0, copy.deepcopy(obj.entity2.attributes_with_structure[0]))

                if obj.entity1.is_weak_entity:
                    obj.attributes_with_structure.insert(0, copy.deepcopy(obj.entity1.attributes_with_structure[1]))
                obj.attributes_with_structure.insert(0, copy.deepcopy(obj.entity1.attributes_with_structure[0]))

                # TODO the code below wouldn't handle a recursive relationship that involves a weak entity
                if obj.attributes_with_structure[0]['attr_name'] == obj.attributes_with_structure[1]['attr_name']:
                    if obj.recursive_relationship_roles:
                        role1, role2 = obj.recursive_relationship_roles
                        obj.attributes_with_structure[0]['attr_name'] = f"{role1[:-3]}_id"
                        obj.attributes_with_structure[1]['attr_name'] = f"{role2[:-3]}_id"
                    else:  
                        # The only way this happens if the relationship is between two subclasses in the same inheritance hierarchy
                        # The rule is that we use the entity (subclass) names in that case
                        obj.attributes_with_structure[0]['attr_name'] = f"{obj.entity1.unique_name}_id"
                        obj.attributes_with_structure[1]['attr_name'] = f"{obj.entity2.unique_name}_id"

                node_data.update({
                    "node_type": "RELATIONSHIP",
                    "recursive_relationship_roles": obj.recursive_relationship_roles,
                    "entity1": obj.entity1.unique_name,
                    "entity2": obj.entity2.unique_name,
                    "attributes": [attr.unique_name for attr in obj.attributes],
                    "tables": list(obj.tables),
                    "attributes_with_structure": obj.attributes_with_structure
                })
            elif isinstance(obj, Attribute):
                node_data.update({
                    "node_type": "ATTRIBUTE",
                    "attr_type": obj.attr_type,
                    "is_multivalued": obj.is_multivalued,
                    "parent_attribute": obj.parent_attribute.unique_name if obj.parent_attribute else None,
                    "entity": obj.entity.unique_name,
                    "children": [child.unique_name for child in obj.children] if obj.is_composite else [],
                    "is_composite": obj.is_composite
                })
            return node_data
        elif isinstance(obj, Edge):
            return {
                "type": "edge",
                "edge_type": obj.edge_type.name,
                "source": obj.source.unique_name,
                "target": obj.target.unique_name
            }
        return super().default(obj)

def serialize_graph(graph: Graph) -> str:
    return json.dumps({
        "nodes": graph.nodes,
        "edges": graph.edges
    }, cls=GraphEncoder, indent=2)

def deserialize_graph(json_str: str) -> Graph:
    data = json.loads(json_str)
    graph = Graph()

    node_map: Dict[str, Node] = {}

    for node_data in data["nodes"]:
        node_type = node_data["node_type"]
        if node_type == "ENTITY":
            node = Entity(
                name=node_data["name"],
                unique_name=node_data["unique_name"]
            )
            node.is_subclass = node_data["is_subclass"]
            node.is_weak_entity = node_data["is_weak_entity"]   
            node.tables = node_data["tables"]
            if node_data["parent_entity"]:
                node.parent_entity = node_map[node_data["parent_entity"]]
            node.temp_attributes_list = node_data["attributes"]
            node.attributes_with_structure = node_data['attributes_with_structure']
            if node.is_subclass:
                node.partially_by_itself = node_data["partially_by_itself"]
                node.all_by_itself = node_data["all_by_itself"]
                node.contained_in_parent = node_data["contained_in_parent"]
        elif node_type == "RELATIONSHIP":
            node = Relationship(
                name=node_data["name"],
                unique_name=node_data["unique_name"]
            )
            node.tables = node_data["tables"]
            node.attributes_with_structure = node_data['attributes_with_structure']
            node.recursive_relationship_roles = node_data["recursive_relationship_roles"]
            node.entity1 = node_map[node_data["entity1"]]
            node.entity2 = node_map[node_data["entity2"]] 
            node.temp_attributes_list = node_data["attributes"]
        elif node_type == "ATTRIBUTE":
            node = Attribute(
                name=node_data["name"],
                unique_name=node_data["unique_name"],
                attr_type=node_data["attr_type"],
            )
            node.is_multivalued = node_data["is_multivalued"]
            node.is_composite = node_data["is_composite"]
            if node_data["parent_attribute"]:
                node.parent_attribute = node_map[node_data["parent_attribute"]]
            node.entity = node_map[node_data["entity"]]
            node.temp_children_list = node_data["children"]
        graph.add_node(node)
        node_map[node.unique_name] = node


    # fix the attributes in the nodes for entities and relationships
    for node in graph.nodes:
        if isinstance(node, Entity) or isinstance(node, Relationship):
            node.attributes = [node_map[attr] for attr in node.temp_attributes_list]
        elif isinstance(node, Attribute):
            node.children = [node_map[child] for child in node.temp_children_list]

    for edge_data in data["edges"]:
        source = node_map[edge_data["source"]]
        target = node_map[edge_data["target"]]
        edge = Edge(
            EdgeType[edge_data["edge_type"]],
            source,
            target
        )
        graph.add_edge(edge)

    return graph
