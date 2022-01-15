import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
co2emission_table_drop = "DROP TABLE IF EXISTS co2_emission;"
population_table_drop = "DROP TABLE IF EXISTS population;"
surfacetemp_table_drop = "DROP TABLE IF EXISTS surface_temp;"
naturaldisasterinfo_table_drop = "DROP TABLE IF EXISTS naturaldisaster_info;"
naturaldisasterresult_table_drop = "DROP TABLE IF EXISTS naturaldisaster_result ; "

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
naturaldisasterinfo_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster_info(
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
        geo_locations varchar, 
        latitude FLOAT,
        longitude FLOAT );
         """)
naturaldisasterresults_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster_results(
        naturaldisaster_id SERIAL PRIMARY KEY NOT NULL, 
        year int, 
        country varchar,
        event_name varchar,
        total_deaths FLOAT,
        no_injured FLOAT,
        no_affectED FLOAT,
        no_homeless FLOAT,
        total_affected FLOAT,
        insured_damages FLOAT,
        total_damages FLOAT,
        cpi FLOAT,
        aid_contribution FLOAT,
        OFDA_response varchar);
         """)
# CREATE Staging TABLES
co2emission_copy_table = ("""Copy co2emission (country, code, year, CO2emission)
        from  
        iam_role 
        Csv NOLOAD
        IGNOREHEADER 1 """)

population_copy_table = ("""Copy population (country name, year, population)
        from  
        iam_role 
        Csv NOLOAD
        IGNOREHEADER 1 """)

surfacetemp_copy_table = ("""Copy surfacetemp (dt, AverageTemperature, AverageTemperatureUncertainty, Country)
        from  
        iam_role 
        Csv NOLOAD
        IGNOREHEADER 1 """)

naturaldisaster_copy_table = ("""Copy naturaldisaster(
        naturaldisaster_id, 
        year, 
        country,
        region,
        location,
        seq, 
        glide, 
        gisaster_group,
        gisaster_subgroup,
        diaster_type, 
        disaster_subtype,
        disaster_subsubtype,
        event_name,
        total_deaths,
        no_injured,
        no_affectED,
        no_homeless,
        total_affected,
        insured_damages,
        total_damages,
        cpi,
        geo_locations, 
        latitude,
        longitude,
        aid_contribution,
        OFDA_response
        from
        iam_role
        csv NOLOAD
        IGNOREHEADER 1        """)
# FINAL TABLES (Insert)
co2emission_table_insert = ("""INSERT INTO co2emission 
    (co2emission_id, country, code, year, co2emission BIGINT)
    SELECT DISTINCT 
        co2emssion_id SERIAL ,
        country, 
        code,
        year,
        co2emission 
        FROM co2emission_stage ; 
""")
population_table_insert = ("""INSERT INTO population 
    (population_id, country, year, population BIGINT)
    SELECT DISTINCT 
        population_id SERIAL ,
        country, 
        year,
        population 
        FROM population_stage ; 
""")

surfacetemp_table_insert = ("""INSERT INTO surfacetemp 
    (surfacetemp_id, year, month, country, avg_temperature, avg_temperature_uncertainty )
    SELECT DISTINCT 
        surfacetemp_id SERIAL ,
        year,
        month,
        country, 
        avg_temperature,
        avg_temperature_uncertainty 
        FROM surfacetemp_stage ; 
""")

naturaldiasterinfo_table_insert = ("""INSERT INTO naturaldiaster_info
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
        geo_locations varchar, 
        latitude FLOAT,
        longitude FLOAT
        FROM naturaldisaster ; 
                 """)
naturaldiasterresult_table_insert ("""INSERT INTO naturaldisaster_result
        naturaldisaster_id SERIAL PRIMARY KEY NOT NULL, 
        year int, 
        country varchar,
        region varchar,
        event_name varchar,
        total_deaths FLOAT,
        no_injured FLOAT,
        no_affectED FLOAT,
        no_homeless FLOAT,
        total_affected FLOAT,
        insured_damages FLOAT,
        total_damages FLOAT,
        cpi FLOAT,
        aid_contribution FLOAT,
        OFDA_response varchar
        FROM natural disaster;
         """)
# QUERY LISTS
create_tables_queries = [surfacetemp_create_table, co2emission_create_table, population_create_table, naturaldisaster_create_table]
drop_tables_queries = [surfacetemp_table_drop, co2emission_table_drop, population_table_drop, naturaldisaster_table_drop]
copy_table_queries = [surfacetemp_copy_table, co2emission_copy_table, population_copy_table]
insert_table_queries = [surfacetemp_table_insert, co2emission_table_insert, population_table_insert]