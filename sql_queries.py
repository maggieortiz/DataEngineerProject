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
        co2emission SERIAL PRIMARY KEY NOT NULL,
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
        surfacetemp_id SERIAL PRIMARY KEY NOT NULL,
        year int,
        month int,
        country varchar,
        avg_temperature double, 
        avg_temperature_uncertainty double
        );
        """)
naturaldisaster_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster(
        naturaldisaster_id SERIAL PRIMARY KEY NOT NULL, 
        year int, 
        country varchar,
        region varchar,
        location varchar,
        seq int, 
        glide varchar, 
        gisaster_group varchar,
        gisaster_subgroup varchar,
        diaster_type varchar, 
        disaster_subtype varchar,
        disaster_subsubtype varchar,
        event_name varchar,
        total_deaths FLOAT,
        no_injured FLOAT,
        no_affectED FLOAT,
        no_homeless FLOAT,
        total_affected FLOAT,
        insured_damages FLOAT,
        total_damages FLOAT,
        cpi FLOAT,
        geo_locations varchar, 
        latitude FLOAT,
        longitude FLOAT,
        aid_contribution FLOAT,
        OFDA_response varchar);
         """)
# CREATE TABLES

# FINAL TABLES (Insert)

# QUERY LISTS
create_tables_queries = 
drop_tables_queries = 
copy_table_queries = 
insert_table_queries = 