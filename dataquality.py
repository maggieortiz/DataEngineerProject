
import configparser
import psycopg2
from sql_queries import rowcolcheck_queries
import pandas as pd
import numpy as np

def checkrownumb(cur, conn):
    # naturaldisaster, surface temp, population, co2emissions
    pd_d = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\DISASTERS\\1900_2021_DISASTERS.csv')
    pd_temp = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\GlobalLandTemperaturesByCountry.csv')
    pd_pop = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\Population\\population_total_long.csv')
    pd_em = pd.read_csv("C:\\Users\\msbar\\Data Engineering Final Project\\data\\co2_emission.csv")
    shape = [][]
    shape[0][0] = pd_d.shape[0]
    shape[0][1] = pd_d.shape[1]
    shape[1][0] = pd_temp.shape[0]
    shape[1][1] = pd_temp.shape[1]
    shape[2][0] = pd_pop.shape[0]
    shape[2][1] = pd_pop.shape[1]
    shape[3][0] = pd_em.shape[0]
    shape[3][1] = pd_em.shape[1]
    i = 0 
    for query in rowcolcheck_queries:
        df = cur.execute(query)
        if df.shape[0] == shape[i, 0]
            print("All rows have been entered")
        if df.shape[1] == shape[i, 1]
            print("All columns have been entered")
        i = i + 1
        #conn.commit()


def checknulls(cur, conn):
    pd_d = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\DISASTERS\\1900_2021_DISASTERS.csv')

    for query in row_queries:
        na = pd_d.isna().sum()
        dg = na['Disaster Group']/pd_d.shape[0]
        if dg > 0.0
            print("Disaster Group Nulls Exist")
        else 
            print("Disaster Group contains no Nulls")

        dt = na['Disaster Type']/pd_d.shape[0]
        if dt > 0.0
            print("Disaster Type Nulls Exist")
        else 
            print("Disaster Type contains no Nulls")

         total_deaths = na['Total Deaths']/pd_d.shape[0]
        if total_deaths > 0.3 
            print("More than 30 percent total_death rows are empty")
        else 
            print("Total Deaths are filled out for more than 30 percent of rows")
        
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    checkrownumb(cur, conn)
    checknulls(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()