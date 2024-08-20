from enum import Enum
from typing import List, Tuple, Union, Dict
from sql_parser import parse
from pyparsing import ParseResults
import logging

#####################################
######## CREATE RELATIONSHIP
######################################
def analyze_attribute(attr):
      is_primary_key = 'PRIMARY KEY' in list(attr) 
      is_discriminator = 'DISCRIMINATOR' in list(attr)
      is_multivalued = (len(attr) == 3 and attr[2] == '[]')
      sub_attributes = []
      if attr[1] == 'COMPOSITE':
          for i in range(3, len(attr)-1):
            sub_attributes.append({'attr_name': attr[i][0], 'attr_type': attr[i][1]})
      attr_name = attr[0]
      attr_type = attr[1]
      return {
            'attr_name': attr_name,
            'attr_type': attr_type,
            'is_primary_key': is_primary_key,
            'is_discriminator': is_discriminator,
            'is_multivalued': is_multivalued,
            'sub_attributes': sub_attributes
        }

# Function to convert ParseResults to the desired dictionary format
def convert_parse_results_relationship(parse_results):
    # Extract table name
    table_name = parse_results.table_name[0]
    
    # Extract and format attributes
    attributes = [analyze_attribute(attr) for attr in parse_results.attributes]
    
    # Helper function to process entity modifiers
    def process_entity(entity_name, modifier):
        cardinality = True if modifier.get('cardinality', 'ONE') == 'ONE' else False
        participation = True if modifier.get('participation', 'TOTAL') == 'TOTAL' else False
        role_info = list(modifier.get('role', []))
        if role_info:
            role_name = role_info[0]
        else:
            role_name = None
        return {'name': entity_name, 'one': cardinality, 'total': participation, 'role': role_name}
    
    # Process entity1
    entity1 = process_entity(parse_results.entity1[0], parse_results.entity1_modifier)
    
    # Process entity2
    entity2 = process_entity(parse_results.entity2[0], parse_results.entity2_modifier)
    
    # Construct the final dictionary
    result = {
        'table_name': table_name,
        'entity1': entity1,
        'entity2': entity2,
        'attributes': attributes
    }
    
    return result

#####################################
######## CREATE ENTITY
######################################
class EntityType(Enum):
    REGULAR = 'REGULAR'
    WEAK = 'WEAK'
    SUBCLASS = 'SUBCLASS'

def convert_entity_parse_results(parse_results) -> Dict[str, Union[str, EntityType, List[Tuple[str, str, bool]]]]:
    result = {}
    
    # Determine entity type
    if  'WEAK' in list(parse_results):
        result['entity_type'] = EntityType.WEAK
    elif 'SUBCLASS OF' in list(parse_results):
        result['entity_type'] = EntityType.SUBCLASS
    else:
        result['entity_type'] = EntityType.REGULAR
    
    # Extract table name
    result['table_name'] = parse_results.table_name[0]
    
    # Extract parent entity for weak and subclass entities
    if result['entity_type'] in [EntityType.WEAK, EntityType.SUBCLASS]:
        result['parent_entity'] = parse_results.parent_entity[0]
    
    # Process attributes
    result['attributes'] = [analyze_attribute(attr) for attr in parse_results.attributes]
    
    return result

#####################################
######## INSERT STATEMENT
######################################
def analyze_value(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, ParseResults):
        if value[0] == '(' and value[-1] == ')':
            return tuple(analyze_values(value[1:-1]))
        elif value[0] == '[' and value[-1] == ']':
            return analyze_values(value[1:-1])
    assert False

def analyze_values(values):
    return [analyze_value(v) for v in values]

def analyze_insert(parsed_insert):
    table_name = list(parsed_insert['table_name'])[0]
    values = parsed_insert['values'][1:-1]  # Remove outer parentheses
    analyzed_values = analyze_values(values)
    return {
        'table_name': table_name,
        'values': analyzed_values
    }

def convert_value(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, ParseResults):
        if value[0] == '(' and value[-1] == ')':
            return tuple(convert_values(value[1:-1]))
        elif value[0] == '[' and value[-1] == ']':
            return list(convert_values(value[1:-1]))
    return value

def convert_values(values):
    return [convert_value(v) for v in values]

#####################################
######## ALTER
######################################
def analyze_alter(p):
    return None

#####################################
######## SELECT
######################################
def analyze_select(p):
    lp = list(p)
    return {'table_name': lp[3]}


####################### 
######### OVERALL
####################### 
def parse_and_analyze(s):
    logging.debug(f"Parsing: {s}")
    p = parse(s)
    logging.debug(f"Result: {p}")
    lp = list(p)
    if 'CREATE' in lp and 'ENTITY' in lp:
        return convert_entity_parse_results(p)
    elif 'CREATE RELATIONSHIP' in lp:
        return convert_parse_results_relationship(p)
    elif 'INSERT INTO' in lp:
        return analyze_insert(p)
    elif 'ALTER' in lp:
        return analyze_alter(p)
    elif 'SELECT' in lp:  
        return analyze_select(p)
    else:
        assert False
