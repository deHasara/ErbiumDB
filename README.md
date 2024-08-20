# ErbiumDB: An Entity-Relationship Database

ErbiumDB is a research prototype being built to explore the challenges and opportunities in using Entity-Relationship model as the user-facing data model. ErbiumDB is being built as a layer (written in Python) on top of an existing data storage system, in this case, PostgreSQL.

## Using ErbiumDB

`erbium.py` is the entry point for all the functions. 

1. `python3 erbium.py init <dbname> <jsonfile>` will read the E/R schema from the provide JSON file, and create the requisite tables in the backend PostgreSQL database (dbname), creating it if needed. This command will clear out the database if it already exists, so should be used carefully. See `example.json` file for an example input file, that contains the "create entity" and "create relationship" commands. Currently it also requires manual input of the mapping between the E/R model and the backend relational model ("connected-subgraphs" field).

1. Similarly: `python3 erbium.py insert <dbname> <jsonfile>` will read the insert statements against the E/R model, and will populate the data into the database tables. See `example.json`.

1. Finally, `python3 erbium.py shell <dbname>` will start a shell which accepts queries against the database in an SQL-like language. However, only a few basic queries are supported at this point. For more complex queries, manual translation can be done and the queries can be run directly against the PostgreSQL database using `psql` or some other client.
