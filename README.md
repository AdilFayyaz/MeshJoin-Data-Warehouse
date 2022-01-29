# MeshJoin-Data-Warehouse
A near real time Data Warehouse using the MeshJoin Algorithm 

## Steps to run the project:
### Step 1: 
	Run the createDW.sql file -This will create a new schema with the name 'dwhproject'
	and create the DWH Tables.
### Step 2: 
	Run the Transaction_and_MasterData_Generator.sql file - This will create the transactions and 
	masterdata tables in the same schema 'dwhproject'
### Step 3:
	Open Eclipse. And Click on Import Project from the File Settings
### Step 4:
	Select 'General' and then 'Existing Projects into Workspace'
### Step 5: 
	Select the submitted folder 'meshJoin' as the root directory and click on Finish
### Step 6:
	If required the username and password of MySQL might have to be adjusted.
	Navigate to the DBConnect.java file in the source folder.
	On line 13, enter the Name of the host, and on line 14 enter the password
### Step 7:
	On the navigation bar, click on Run to Run the project
### Step 8: 
	This step is not compulsory, however, if there is an error in running code, it is probably
	because some other projects main file is still configured.
	To configure the current projects file, click on the down arrow next to run, then run
	configurations, then click on new launch configuration, if the project is not imported,
	then click on browse and select your project folder. Then click on search and select the main class.
	Then you can run/click apply. 
### Step 9: 
	Run the queriesDW.sql file to get the DWH analysis

### Note: The steps HAVE to be done in the specific order, for correct execution.
