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
        co2emission int
        );
        """)
population_create_table = ("""CREATE TABLE IF NOT EXISTS population(
        country varchar,
        year int, 
        population int
        );
        """)

# CREATE TABLES

# FINAL TABLES (Insert)

# QUERY LISTS
create_tables_queries = 
drop_tables_queries = 
copy_table_queries = 
insert_table_queries = 