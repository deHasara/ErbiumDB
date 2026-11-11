
# ErbiumDB

ErbiumDB is a fundamentally new approach to database design that explores the benefits of adopting the Entity–Relationship (E/R) model as a primary, user-facing abstraction for data management. Rather than requiring users to interact directly with relational schemas, ErbiumDB enables them to define and manipulate data using SQL over high-level conceptual constructs such as entities and relationships.

The system is implemented as a lightweight Python layer on top of an existing relational database system, currently PostgreSQL. This architecture allows ErbiumDB to leverage the maturity and robustness of a well-established storage engine while focusing its research contributions on schema mapping, query translation, and workload-aware, cost-based optimization of relational schemas. By bridging the gap between conceptual modeling and logical design, ErbiumDB provides a platform for systematically studying how high-level data models can be translated into efficient relational representations across diverse workloads, while enabling full logical independence.

---

## Repository Structure

- `prototype/src/` — Core implementation
   - `prototype/src/test_file-1.py` — Entry point
- `prototype/src/dist/test_file-1` — Main executable  
- `prototype/src/example2_e_commerce.json` — Example input schema and workload

---

## Requirements


- PostgreSQL (tested with PostgreSQL 16)  
- Python dependencies (if building from sources)
   - Python 3.x (tested with Python 3.12)
   - install via `requirements.txt`.

---

## Using ErbiumDB

1. **Input E/R Schema**

   The input E/R schema is provided through a JSON file, for example: `prototype/src/example2_e_commerce.json`.
   
   This file contains:
    - The input E/R schema in schema DDL  
    - Metadata required to initialize the database  
    - Query workloads (SELECT and INSERT)  

2. **Generate the mapping & initialize the Database**

    Run:
    
    ```bash
    ./test_file-1 init <dbname> <jsonfile>
    ```

    This command:

      - Reads the E/R schema from the JSON file  
      - Generates the dataset, including SELECT and INSERT workloads  
      - Determines the best relational mapping based on the workload  
      - Creates the required tables and metadata for mapping
      - Persists the resulting schema and data in the PostgreSQL database  
      
      > **Warning:** If the database already exists, it will be cleared and recreated.
      
      > **Assumption:** PostgreSQL has a user postgres with password 'password'
      
      **Example:**
      
      ```bash
      ./test_file-1 init test_db example2_e_com_small.json
      ```
3. **Run Queries**

   Run:

   ```bash
   ./test_file-1 run_queries <dbname>
   ```
   
   This command starts an interactive shell that accepts SQL queries over the conceptual schema.
    
      - ```SELECT * FROM <entity_or_relationship>```
      - ```INSERT INTO <entity_or_relationship>```

    **Example:**
      
      ```bash
      ./test_file-1 run_queries test_db
      ```

    
    
   
      
