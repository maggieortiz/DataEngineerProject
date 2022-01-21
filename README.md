# Natural Disaster Countries vs CO2 emissions - Data Engineering Nanodegree
- Final Project for the Data Engineering Nano-degree w/ Udacity

### Script Goal: 
- Take these .csv data files and create an ETL to migrate data to tables in redshift 

### Steps: 
1. Add CSV files to an S3 bucket
1. Create Redshift and IAM role in AWS portal
2. Run 'ETL Testing Notebook'
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


### Queries: 
- Count Natural Diasters by country
- GroupBy Natural Disaster type & look at deaths & damages to find the worst type of disaster 
- Table with natural disaster count by country and order by population 
- 

### Graphs: 
- Graph natural Disater by emission
- emission by population 

