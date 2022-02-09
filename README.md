# Natural Disaster Countries vs CO2 emissions - Data Engineering Nanodegree
- Final Project for the Data Engineering Nano-degree w/ Udacity

### Script Goal: 
- Take these .csv data files and create an ETL to migrate data to tables in redshift 
- Check out Data_Model.ipynb to see the final Data Model structure

### PostGreSQL Steps: 
- Without access to AWS I wanted to create a POSTGRESQL local host db to do data analysis
1. Go to PostGreSQL_dataqueries.ipynb file 
2. run scripts to create tables
3. run scripts to insert data into tables 
4. run queries to execute queries 
5. run visualization and ML analysis cells

### AWS Steps: 
1. Add CSV files to an S3 bucket
1. Create Redshift and IAM role in AWS portal
2. Run 'ETL Testing Notebook'
    a. Run create_table.py - create tables
    b. Run etl.py - copy data from csv to redshift
    c. Run data_quality.py - check for empty tables and null values 
3. Perform the queries and analysis in question 
4. Close Redshift cluster and S3 bucket in AWS portal

### Tables: 
- Co2emissions - emissions by country & year 
- Population - population by country & year 
- Surface Temperature - surfacetemp by country & year 
- Natural Disaster - disasters by country & year 
    - Total Death Data 
    - Total Damages
    - Number of people Injured 
    - Number of people affected
    - and much more information about the damages 

### Questions: 
1. What will the data be used for? 
    - Data will be used to look at correlation between natural disaster, co2 emissions for that country, population, number of deaths, and the rate of change of certain surface temperatures. 
2. How the user use this data model?
    - After creating redshift data, user can query data as they wish to find results to particular questions, or create visualizations. 
3. Why did you choose this approach? 
    - AWS datawarehouse is a structured way to store large amounts of data which I think is useful for answering a variety of questions while keeping the data in a structured form. 
4. How often should this data be updated? 
    - Yearly. The population, natural disaster, co2emissions are yearly based and while the surface temp is a monthly statistic, I think aggregating it yearly would make sense for the scope of this analysis.  
5. The data was increased by 100x.
    - As long as the data update is monthly or yearly like suggested I think a datawarehouse could handle it. 
6. The pipelines would be run on a daily basis by 7 am every day.
    - I think this set up would handle being run every day. 
7. The database needed to be accessed by 100+ people. 
    - I think it might need to be changed to a Spark data lake to handle the mulitple access points. 

### Queries: 
- Count Natural Diasters by country
- GroupBy Natural Disaster type & look at deaths & damages to find the worst type of disaster 
- Table with natural disaster count by country and order by population 

### Graphs: 
- Graph natural Disater by emission
- emission by population 

