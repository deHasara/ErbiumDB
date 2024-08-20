import re
from typing import List, Dict, Tuple, Any

def generate_sql_query(tables: List[Tuple[str, List[List[str]]]], entity, graph):
    # relevant_tables is the list of tables we would use for "inserts" -- so let's start with that
    relevant_tables = [table for table in tables if table[0] in entity.tables]

    # construct attribute lists for the relevant tables since we will need them often
    relevant_table_attribute_lists = {}
    for table in relevant_tables:
        relevant_table_attribute_lists[table[0]] = [column[0] for column in table[1]]

    attributes_with_structure = entity.attributes_with_structure

    select_clause = []
    from_clause = [relevant_tables[0][0]] # TODO We are assuming that the first table is the main table
    for table in relevant_tables[1:]:
        from_clause.append(f"JOIN {table[0]} ON {relevant_tables[0][0]}.{relevant_tables[0][1][0][0]} = {table[0]}.{table[1][0][0]}")

    for attr in attributes_with_structure: 
        attr_name = attr["attr_name"]
        # if composite, check if it may have been split into multiple columns
        found = [t for t in relevant_table_attribute_lists if attr_name in relevant_table_attribute_lists[t]]
        if attr["attr_type"] == 'COMPOSITE': 
            if found:
                # not split up
                select_clause.append(f"{found[0]}.{attr_name} AS {attr_name}")
            else: 
                # look for attr_name__ in the attribute lists
                split_parts = []
                for t in relevant_table_attribute_lists:
                    for a in relevant_table_attribute_lists[t]:
                        if f"{attr_name}__" in a:
                            split_parts.append(a)
                select_clause.extend([f"{t} AS {t}" for t in split_parts])
        elif attr["is_multivalued"]:
            # if the attribute has been normalized away, then it is in its own table which doesn't have [] 
            assert found
            if len(relevant_table_attribute_lists[found[0]]) == 2:
                select_clause.append(f"ARRAY_AGG({attr_name}) AS {attr_name}")
            else: 
                select_clause.append(f"{attr_name} AS {attr_name}")
        else:
            select_clause.append(f"{found[0]}.{attr_name} AS {attr_name}")

    select_clause_str = ", ".join(select_clause)
    from_clause_str = " ".join(from_clause)

    # check if group by is needed
    if "AGG" in select_clause_str:
        group_by_clause = [c.split()[0] for c in select_clause if "AGG" not in c]
        group_by_clause_str = ", ".join(group_by_clause)
        return f"SELECT {select_clause_str} FROM {from_clause_str} GROUP BY {group_by_clause_str}"
    else: 
        return f"SELECT {select_clause_str} FROM {from_clause_str}"
