
import configparser
import psycopg2
from sql_queries import rowcolcheck_queries , checknull_queries
import pandas as pd
import numpy as np

def checkrownumb(cur, conn):
    text = ['naturaldisaster', 'surface temp', 'population', 'co2emissions']
    pd_d = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\DISASTERS\\1900_2021_DISASTERS.csv')
    pd_temp = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\GlobalLandTemperaturesByCountry.csv')
    pd_pop = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\Population\\population_total_long.csv')
    pd_em = pd.read_csv("C:\\Users\\msbar\\Data Engineering Final Project\\data\\co2_emission.csv")
    shape = [0,0]
    shape.insert(0, [pd_d.shape[0], pd_d.shape[1] ])
    shape.insert(1, [pd_temp.shape[0], pd_temp.shape[1] ])
    shape.insert(2, [pd_pop.shape[0], pd_pop.shape[1] ])
    shape.insert(3, [pd_em.shape[0], pd_em.shape[1] ])

    i = 0 
    for query in rowcolcheck_queries:
        df = pd.read_sql_query(query, conn)
        if df.shape[0] == shape[i][0]:
            print("All rows of " + text[i] + " have been entered")
        if df.shape[1] == shape[i][1]:
            print("All columns of " + text[i] + " have been entered")
        i = i + 1
        #conn.commit()


def checknulls(cur, conn):
    pd_d = pd.read_csv('C:\\Users\\msbar\\Data Engineering Final Project\\data\\DISASTERS\\1900_2021_DISASTERS.csv')
    na = pd_d.isna().sum()
    dg = na['Disaster Group']/pd_d.shape[0]
    dt = na['Disaster Type']/pd_d.shape[0]
    total_deaths = na['Total Deaths']/pd_d.shape[0]

    for query in checknull_queries:
        df = pd.read_sql_query(query, conn)
        na_f = df.isna().sum()
        dg_f = na_f['disaster_group']/df.shape[0]
        dt_f = na_f['disaster_type']/df.shape[0]
        total_deaths_f = na_f['total_deaths']/df.shape[0]
        if dg_f > 0:
            print("Disaster Group Nulls Exist")
        else:
            print("Disaster Group contains no Nulls")

        if dt_f > 0:
            print("Disaster Type Nulls Exist")
        else: 
            print("Disaster Type contains no Nulls")

        if total_deaths_f == total_deaths:
            print("Percentage of Null in total_death column is equal to input")
        else:
            print("Percentage of Null in total_death column is NOT equal to input")
        
        #cur.execute(query)
        #conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    checkrownumb(cur,conn)
    checknulls(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()