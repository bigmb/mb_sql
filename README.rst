======
mb_sql
======

A Python package for SQL database operations, replication, and snapshot management.
Built on top of SQLAlchemy, it provides convenient utilities for connecting to databases, executing queries, managing schemas/tables, syncing data between sources, and taking DataFrame snapshots to S3.

.. contents:: Table of Contents
   :depth: 2

Installation
============

.. code-block:: bash

   pip install mb_sql

Requirements
------------

- Python >= 3.8
- sqlalchemy
- numpy
- pandas
- mb_utils
- mb_pandas
- mb_base

Modules
=======

conn -- Connection Management
-----------------------------

Create and manage SQLAlchemy engine, session, base, and metadata objects.

.. code-block:: python

   from mb.sql.conn import get_engine, get_session, get_base, get_metadata

   # Create an engine (defaults to PostgreSQL on localhost)
   engine = get_engine(
       name='postgresql+psycopg2',
       db='my_database',
       user='postgres',
       password='postgres',
       host='localhost',
       port=5432,
   )

   # Create a session
   session = get_session(engine)

   # Create a declarative base
   base = get_base()

   # Create metadata
   metadata = get_metadata(base, engine)

**Functions:**

- ``get_engine(name, db, user, password, host, port, logger, echo)`` -- Create a SQLAlchemy engine.
- ``get_session(engine, logger)`` -- Create a SQLAlchemy session.
- ``get_base(logger)`` -- Create a SQLAlchemy declarative base.
- ``get_metadata(base, conn, logger)`` -- Create and bind metadata.

basic -- Core SQL Operations
----------------------------

Read from and write to SQL databases using pandas DataFrames.

.. code-block:: python

   from mb.sql.basic import read_sql, to_sql, engine_execute

   # Read a SQL query into a DataFrame
   df = read_sql("SELECT * FROM my_table", engine, chunk_size=10000)

   # Write a DataFrame to a SQL table
   to_sql(df, engine, 'my_table', schema='public', if_exists='replace')

   # Execute a raw query
   engine_execute(engine, "DELETE FROM my_table WHERE id = 1")

**Functions:**

- ``read_sql(query, engine, index_col, chunk_size, logger)`` -- Read a SQL query into a DataFrame (supports chunked reads).
- ``to_sql(df, engine, table_name, schema, if_exists, index, index_label, chunksize, logger)`` -- Write a DataFrame to a SQL table.
- ``engine_execute(engine, query_str)`` -- Execute a raw SQL statement on an engine or connection.

utils -- Schema & Table Utilities
---------------------------------

Manage database schemas, tables, indexes, and perform database cloning.

.. code-block:: python

   from mb.sql.utils import list_schemas, list_tables, rename_table, drop_table

   # List all schemas
   schemas = list_schemas(engine)

   # List tables in a schema
   tables = list_tables(engine, schema='public')

   # Rename a table
   rename_table('new_name', 'old_name', engine, schema='public')

   # Drop a table
   drop_table('my_table', engine, schema='public')

**Functions:**

- ``list_schemas(engine, logger)`` -- List all schemas in a database.
- ``list_tables(engine, schema, logger)`` -- List all tables in a schema.
- ``rename_table(new_table_name, old_table_name, engine, schema, logger)`` -- Rename a table.
- ``drop_table(table_name, engine, schema, logger)`` -- Drop a table.
- ``drop_schema(schema, engine, logger)`` -- Drop a schema.
- ``create_schema(schema, engine, logger)`` -- Create a schema.
- ``create_index(table, index_col, engine, logger)`` -- Create an index on a table.
- ``clone_db(ori_db_location, copy_db_location, logger)`` -- Clone a PostgreSQL database using ``pg_dump``.

update -- Data Synchronization
------------------------------

Synchronize data between source and destination databases, with S3 integration for detecting new/updated records.

.. code-block:: python

   from mb.sql.update import get_last_updated_timestamp, update

   # Get the latest timestamp in a table
   last_ts = get_last_updated_timestamp(engine, 'my_table', schema='public')

**Functions:**

- ``get_last_updated_timestamp(engine, table_name, schema, updated_col)`` -- Get the most recent update timestamp from a table.
- ``get_last_updated_data_id(engine, table_name, schema, updated_col)`` -- Get all IDs with their update timestamps.
- ``get_new_updated_timestamps_from_s3(bucket_name, key, last_updated, ...)`` -- Fetch new records from S3 since a given timestamp.
- ``update(df, mutable_name, mutable_table, index_col, is_new_table)`` -- Upsert a DataFrame into a destination table.

snapshot -- DataFrame Snapshots
-------------------------------

Save and load versioned DataFrame snapshots to/from S3.

.. code-block:: python

   from mb.sql.snapshot import save, load, list_names, list_ids

   # List available snapshot names
   names = list_names()

   # Save a snapshot
   save(df, 'my_dataframe')

   # Load the latest snapshot
   df = load('my_dataframe')

   # Load a specific snapshot by ID
   df = load('my_dataframe', sn_id=260101)

**Functions:**

- ``get_s3url(df_name, id, location, file_type)`` -- Build the S3 URL for a snapshot.
- ``list_names(location)`` -- List DataFrame names with available snapshots.
- ``list_ids(df_name, show_progress)`` -- List snapshot IDs for a given DataFrame.
- ``latest_id(df_name)`` -- Get the most recent snapshot ID.
- ``make_new_id(df_name)`` -- Generate a new snapshot ID based on the current date.
- ``save(df, df_name, sn_id, local_path, logger)`` -- Save a DataFrame snapshot to S3.
- ``load(df_name, sn_id, logger)`` -- Load a DataFrame snapshot from S3.

tables -- Table Configuration
-----------------------------

Define ``TableConfig`` objects to describe source/destination table mappings for replication workflows.

.. code-block:: python

   from mb.sql.tables import TableConfig

   config = TableConfig(
       schema='my_schema',
       table='my_table',
       index_col='id',
       chunk_size=10000,
       updated_col='updated_at',
       dtype={'id': sa.Integer, 'name': sa.Text},
   )

slack -- Notifications
----------------------

Send messages to Slack channels via webhooks.

.. code-block:: python

   from mb.sql.slack import slack_msg

   slack_msg(
       webhook='https://hooks.slack.com/services/...',
       msg={'text': 'Database sync complete!'},
   )

**Functions:**

- ``slack_msg(webhook, msg, logger)`` -- Post a JSON message to a Slack webhook.

