import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
co2emission_table_drop = "DROP TABLE IF EXISTS co2emission;"
population_table_drop = "DROP TABLE IF EXISTS population;"
surfacetemp_table_drop = "DROP TABLE IF EXISTS surfacetemp;"
naturaldisasterinfo_table_drop = "DROP TABLE IF EXISTS naturaldisaster_info;"
naturaldisasterdamage_table_drop = "DROP TABLE IF EXISTS naturaldisaster_damage ; "
co2emission_stage_table_drop = "DROP TABLE IF EXISTS co2emission_stage;"
population_stage_table_drop = "DROP TABLE IF EXISTS population_stage;"
surfacetemp_stage_table_drop = "DROP TABLE IF EXISTS surfacetemp_stage;"
naturaldisaster_stage_table_drop = "DROP TABLE IF EXISTS naturaldisaster_stage;"
# CREATE TABLES
co2emission_create_table = ("""CREATE TABLE IF NOT EXISTS co2emssion(
        country varchar NOT NULL,
        code varchar,
        year int NOT NULL, 
        co2emission FLOAT
                );
        """)
population_create_table = ("""CREATE TABLE IF NOT EXISTS population(
        country varchar NOT NULL,
        year int NOT NULL, 
        population int
        );
        """)

surfacetemp_create_table = ("""CREATE TABLE IF NOT EXISTS surfacetemp(
        year int NOT NULL,
        month int,
        country varchar NOT NULL,
        avg_temperature FLOAT, 
        avg_temperature_uncertainty FLOAT
        );
        """)
naturaldisasterinfo_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster_info(
        year int NOT NULL, 
        country varchar,
        region varchar,
        location varchar(5000),
        seq int, 
        glide varchar, 
        gisaster_group varchar,
        gisaster_subgroup varchar,
        diaster_type varchar, 
        disaster_subtype varchar,
        disaster_subsubtype varchar,
        event_name varchar,
        geo_locations varchar(5000), 
        latitude varchar,
        longitude varchar );
         """)
naturaldisasterdamage_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster_damage(
        year int NOT NULL, 
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
# CREATE STAGING TABLES
co2emission_create_stage_table = ("""CREATE TABLE IF NOT EXISTS co2emission_stage(
        country varchar,
        code varchar,
        year int, 
        CO2emission FLOAT
        );
        """)
population_create_stage_table = ("""CREATE TABLE IF NOT EXISTS population_stage(
        country varchar,
        year int, 
        population int
        );
        """)

surfacetemp_create_stage_table = ("""CREATE TABLE IF NOT EXISTS surfacetemp_stage(
        dt DATE,
        AverageTemperature FLOAT, 
        AverageTemperatureUncertainty FLOAT, 
        Country varchar
        );
        """)
naturaldisaster_staging_create_table = ("""CREATE TABLE IF NOT EXISTS naturaldisaster_stage(
        year int, 
        seq int, 
        glide varchar,
        disaster_group varchar,
        disaster_subgroup varchar,
        diaster_type varchar, 
        disaster_subtype varchar,
        disaster_subsubtype varchar,
        event_name varchar,
        country varchar,
        iso varchar,
        region varchar,
        continent varchar,
        location varchar(5000),
        origin varchar, 
        associated_dis varchar,
        associated_dis2 varchar,
        OFDA_response varchar, 
        appeal varchar,
        declaration varchar,
        aid_contribution FLOAT,
        dis_mag_value FLOAT,
        dis_mag_scale varchar,
        latitude varchar,
        longitude varchar,
        local_time varchar,
        river_basin varchar(1000),
        start_year BIGINT,
        start_month varchar,
        start_day int,
        end_year BIGINT,
        end_month varchar,
        end_day int,
        total_deaths FLOAT,
        no_injured FLOAT,
        no_affected FLOAT,
        no_homeless FLOAT,
        total_affected FLOAT,
        insured_damages FLOAT,
        total_damages FLOAT,
        cpi FLOAT,
        adm_level varchar,
        admin1_code varchar(5000), 
        admin2_code varchar(5000), 
        geo_locations varchar(5000)
        );
         """)
# CREATE Staging TABLES
co2emission_copy_table = ("""Copy co2emission_stage
        from {}
        iam_role {}
        Csv NOLOAD
        IGNOREHEADER 1
        delimiter ',' ;""").format(config['S3']['CSV_CO2EMISSION'], config['IAM_ROLE']['ARN'] )

population_copy_table = ("""Copy population_stage
        from  {}
        iam_role {} 
        Csv NOLOAD
        IGNOREHEADER 1
        delimiter ',' ;""").format(config['S3']['CSV_POPULATION'], config['IAM_ROLE']['ARN'])

surfacetemp_copy_table = ("""Copy surfacetemp_stage
        from {}
        iam_role {}
        Csv NOLOAD
        IGNOREHEADER 1 
        delimiter ','
         ;""").format(config['S3']['CSV_SURFACETEMP'], config['IAM_ROLE']['ARN'])

naturaldisaster_copy_table = ("""Copy naturaldisaster_stage
        from {}
        iam_role {}
        csv NOLOAD
        IGNOREHEADER 1 
        delimiter ','      ; """).format(config['S3']['CSV_NATURALDISASTER'], config['IAM_ROLE']['ARN'])
# FINAL TABLES (Insert)
co2emission_table_insert = ("""INSERT INTO co2emission 
    (country, code, year, co2emission BIGINT)
    SELECT DISTINCT 
        country NOT NULL, 
        code,
        year NOT NULL,
        co2emission 
        FROM co2emission_stage ; 
""")
population_table_insert = ("""INSERT INTO population 
    (country, year, population BIGINT)
    SELECT DISTINCT 
        country NOT NULL, 
        year NOT NULL,
        population 
        FROM population_stage ; 
""")

surfacetemp_table_insert = ("""INSERT INTO surfacetemp 
    (year, month, country, avg_temperature, avg_temperature_uncertainty )
    SELECT DISTINCT 
        extract(year from dt) as year NOT NULL,
        EXTRACT(month from dt) as month,
        Country as country NOT NULL, 
        AverageTemperature as avg_temperature,
        AverageTemperatureUncertainty as avg_temperature_uncertainty 
        FROM surfacetemp_stage ; 
""")

naturaldisasterinfo_table_insert = ("""INSERT INTO naturaldiaster_info  
        year int NOT NULL, 
        country varchar NOT NULL,
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
        FROM naturaldisaster_stage ; 
                 """)
naturaldisasterdamage_table_insert = ("""INSERT INTO naturaldisaster_damage
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
        FROM naturaldisaster_stage;
         """)
# QUERY LISTS
create_table_queries = [naturaldisaster_staging_create_table , co2emission_create_stage_table, population_create_stage_table, surfacetemp_create_stage_table, surfacetemp_create_table, co2emission_create_table, population_create_table, naturaldisasterinfo_create_table, naturaldisasterdamage_create_table]
drop_table_queries = [naturaldisaster_stage_table_drop, surfacetemp_stage_table_drop, population_stage_table_drop, co2emission_stage_table_drop, surfacetemp_table_drop, co2emission_table_drop, population_table_drop, naturaldisasterinfo_table_drop, naturaldisasterdamage_table_drop]
copy_table_queries = [surfacetemp_copy_table, co2emission_copy_table, population_copy_table, naturaldisaster_copy_table]
insert_table_queries = [surfacetemp_table_insert, co2emission_table_insert, population_table_insert, naturaldisasterdamage_table_insert, naturaldisasterinfo_table_insert]