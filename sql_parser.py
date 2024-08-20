from pyparsing import *

# Helper functions
def as_list(t):
    return list(t)

# Define keywords
keywords = ["CREATE", "ENTITY", "RELATIONSHIP", "BETWEEN", "AND", "ONE", "MANY", "TOTAL", "PARTIAL"]

# Create a case-insensitive keyword set
keyword_set = set(keyword.lower() for keyword in keywords)

# Custom identifier that excludes keywords
def custom_identifier():
    return ~MatchFirst(CaselessKeyword(kw) for kw in keywords) + Word(alphas, alphanums + "_")

identifier = custom_identifier()

#####################################
######## Entities
######################################

# Forward declaration for nested attributes
attribute = Forward()
integer = Word(nums)
string_literal = QuotedString("'", escChar="\\")
literal = string_literal | integer

# Data types
data_type = oneOf("INT VARCHAR BOOLEAN DATE", caseless=True)

# Simple attribute
simple_attribute = Group(
    identifier
    + data_type
    + Optional("[]")
    + Optional(CaselessKeyword("PRIMARY KEY"))
    + Optional(CaselessKeyword("DISCRIMINATOR"))
)

# Composite attribute
composite_attribute = Group(
    identifier
    + CaselessKeyword("COMPOSITE")
    + "(" + delimitedList(attribute) + ")"
)

# Define attribute to be either simple or composite
attribute << (composite_attribute | simple_attribute)

# Attribute list
attribute_list = Group(delimitedList(attribute))

# Entity table creation
create_entity_table = (
    CaselessKeyword("CREATE")
    + Optional(CaselessKeyword("WEAK"))
    + CaselessKeyword("ENTITY")
    + identifier("table_name")
    + Optional(CaselessKeyword("DEPENDS ON") + identifier("parent_entity"))
    + Optional(CaselessKeyword("SUBCLASS OF") + identifier("parent_entity"))
    + "(" + attribute_list("attributes") + ")"
    + Optional(";")
)

#####################################
######## Relationships
######################################
# Helper for entity modifiers
entity_modifier = Group(
    Optional(identifier("role"))
    + (CaselessKeyword("ONE") | CaselessKeyword("MANY"))("cardinality")
    + (CaselessKeyword("TOTAL") | CaselessKeyword("PARTIAL"))("participation")
)

# Relationship table creation
create_relationship_table = (
    CaselessKeyword("CREATE RELATIONSHIP")
    + identifier("table_name")
    + "(" + Optional(attribute_list("attributes")) + ")"
    + CaselessKeyword("BETWEEN")
    + identifier("entity1")
    + "(" + entity_modifier("entity1_modifier") + ")"
    + CaselessKeyword("AND")
    + identifier("entity2")
    + "(" + entity_modifier("entity2_modifier") + ")"
)

#####################################
######## SELECT
######################################

# Basic elements
wildcard = Literal("*")

# Nested select items
select_item = Forward()
select_item << (
    Group(Literal("(") + delimitedList(select_item) + Literal(")"))
    | Group(Literal("[") + delimitedList(select_item) + Literal("]"))
    | identifier
)

# Join condition
join_condition = identifier

# Table factor (either a simple table or a parenthesized join)
table_factor = Forward()

# Join expression
join_expr = Forward()

# Define table_factor as either a simple identifier or a parenthesized join_expr
table_factor << (identifier | (Literal("(") + join_expr + Literal(")")))

# Define join_expr as a series of joins
join_expr << (
    table_factor("left")
    + ZeroOrMore(
        CaselessKeyword("JOIN")
        + identifier("right")
        + CaselessKeyword("ON")
        + join_condition("condition")
    )
)

# From clause
from_clause = join_expr

# Select statement (simplified for this example)
select_stmt = (
    CaselessKeyword("SELECT")
    + (Literal("*") | delimitedList(identifier))("columns")
    + CaselessKeyword("FROM")
    + from_clause("from_clause")
    + Optional(CaselessKeyword("WHERE") + SkipTo(StringEnd())("condition"))
)


#####################################
######## INSERT STAT
######################################
# Basic elements
# Define a floating-point number
point = Literal('.')
e = CaselessLiteral('E')
plusorminus = Literal('+') | Literal('-')
number = Combine(
    Optional(plusorminus) +
    Word(nums) +
    Optional(point + Optional(Word(nums))) +
    Optional(e + Optional(plusorminus) + Word(nums))
)

# Value item (can be nested)
value_item = Forward()
value_item << (
    Group(Literal("(") + delimitedList(value_item) + Literal(")"))
    | Group(Literal("[") + delimitedList(value_item) + Literal("]"))
    | string_literal
    | number
    | identifier
)

# Insert statement
insert_stmt = (
    CaselessKeyword("INSERT INTO")
    + identifier("table_name")
    + CaselessKeyword("VALUES")
    + Group(Literal("(") + delimitedList(value_item) + Literal(")"))("values")
)

# Parse action to convert parsed results to a more manageable format
# insert_stmt.setParseAction(lambda t: dict(t))

#####################################
######## ALTER TABLE
######################################
# Alter table statement
alter_table = (
    CaselessKeyword("ALTER TABLE")
    + identifier("table_name")
    + (
        (CaselessKeyword("ADD") + attribute("new_attribute"))
        | (CaselessKeyword("MODIFY RELATIONSHIP") + CaselessKeyword("TO") + oneOf("ONE-TO-ONE ONE-TO-MANY MANY-TO-MANY", caseless=True)("new_relationship_type"))
    )
)

# Full SQL statement
sql_stmt = (
    create_entity_table
    | create_relationship_table
    | select_stmt
    | alter_table
    | insert_stmt
)

# Returns a Parsed object
def parse(stmt):
    return sql_stmt.parseString(stmt)

# Parse action to convert parsed results to a more manageable format
#sql_stmt.setParseAction(as_list)
