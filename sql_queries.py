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

# CREATE TABLES

# FINAL TABLES (Insert)

# QUERY LISTS
create_tables_queries = 
drop_tables_queries = 
copy_table_queries = 
insert_table_queries = 