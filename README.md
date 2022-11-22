# Case-Study-Sales_CustomerDemographics
<h3><b>Aim</b></h3> 
 To perform analytics on Sales and Customer Demographics data using Azure Databricks and PowerBI</p>
 
<h3><b>Problem Statement</b></h3> 
We will dig deeper into some of the Databricks features for this project. As for transformation spark is the most popular tools in big data. This is due to the years of experience and expertise put into training, acceptance, tooling, standard development, and re-engineering. So, in many circumstances, employing these excellent spark tools to access data may answer many analytical queries.

This big data project will look at capabilities to run analytical queries on massive datasets. We will use the dataset in a SQL database for this project, and we'll need to ingest and modify the data. We'll use sales and Customer demographics data to perform analysis and answer the following questions:
<li>Total Purchase based on YearlyIncome and Education</li><br>
<li>Numbers of cars Owned based on YearlyIncome</li><br>
<li>Total Purchase based on CommuteDistance and Occupation</li><br>

<h3>Technologies used to work in project</h3>
<ul>
<h4><li>Azure SQL Database</li></h4>
<h4><li>Azure Databricks</li></h4>
<h4><li>Azure Data Factory</li></h4>
<h4><li>PowerBI</li></h4>
</li> 
</ul>


<h3>Data Source Description</h3>
<p> In this project, we will be using Customer test, Individual test tables from this database. </p>

<p>&nbsp;&nbsp; &nbsp;&nbsp;  <b>Customer</b>: This table contain all customer data related information.</p>
<p>&nbsp;&nbsp; &nbsp;&nbsp;  <b>Individual</b>: This table contain all Individual data information.</p>

![tables drawio](https://user-images.githubusercontent.com/64693763/203236419-e246542b-a8f3-4b5d-8331-6be20cbd99b1.png)


<p>&nbsp;&nbsp; &nbsp;&nbsp;  <b>Cust_Ind</b>: This is the final table after joining and performing transformations on above tables.</p>

![finaltable](https://user-images.githubusercontent.com/64693763/203236845-57f87492-6d10-4637-885c-6be7f8c280e6.png)


# Project Architecture

![Customer_demographics_project_architecture](https://user-images.githubusercontent.com/64693763/202996156-b6fc0e9f-7efa-485d-9a05-e50300f28451.png)

# Steps performed to achive the task
<ul>
<li>Create tables in Azure SQL database.</li><br>
<li>Load data in Azure SQL Database.</li><br>
<li>Connect tables in databricks and create dataframes.</li><br>
<li>Join both the dataframes in databricks.</li><br>
<li>Perform transformation and cleaning in databricks using pyspark.</li><br>
<li>Load the data back in new table in SQL database.</li><br>
<li>Connect data with PowerBI.</li><br>
<li>Perform analytics on Sales and Customer demographics data Using PowerBI.</li><br>
</ul>
