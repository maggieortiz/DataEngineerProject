import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
co2emission_table_drop = "DROP TABLE IF EXISTS co2_emission;"
population_table_drop = "DROP TABLE IF EXISTS population;"
surfacetemp_table_drop = "DROP TABLE IF EXISTS surface_temp;"
naturaldisaster_table_drop = "DROP TABLE IF EXISTS natural_disaster;"

# CREATE TABLES
co2emission_create_table = ("""CREATE TABLE IF NOT EXISTS co2emssion(
        country varchar,
        code varchar(3),
        year int, 
        co2emission BIGINT
                );
        """)
population_create_table = ("""CREATE TABLE IF NOT EXISTS population(
        population_id SERIAL PRIMARY KEY NOT NULL,
        country varchar,
        year int, 
        population int
        );
        """)

surfacetemp_create_table = ("""CREATE TABLE IF NOT EXISTS surfacetemp(
        year int,
        month int,
        country varchar,
        avg_temperature double, 
        avg_temperature_uncertainty double
        );
        """)

# CREATE TABLES

# FINAL TABLES (Insert)

# QUERY LISTS
create_tables_queries = 
drop_tables_queries = 
copy_table_queries = 
insert_table_queries = 