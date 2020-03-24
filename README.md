# CSV Coding Challenge
## Objective
 Consumes CSV and inserts rows into SQLite database table
 
## Running the program 
 The run.bat file builds and runs the main class.
 
 Running the program outside of the batch file will require adding the JDBC to the classpath.
 
 In /test there is a sample data.csv with the resulting database, log and -bad.csv files. 
 
## Design Notes
 The program is designed to consume a csv file row by row and insert each row that has the proper number of columns into an SQLite3 database.
 
 After runtime, users can view results in the <filename>.log file. Any rejected rows will be added to <filename>-bad.csv.
    
 Rejected rows include both rows with too many columns and rows with too few columns.
    
 The user is prompted for the filename/filepath to the csv file. This path is then used to create the log and -bad.csv files.
 
 The program automatically creates a table in the database called table1 to store the csv data.
 
 When I built a progress bar for the UI, I decided not to use another thread, because I wanted to keep things simple; but to optimize runtime, I only update the bar when its length changes.
 
 Because the program handles raw input, it tests the input before use. (i.e. removes filename extensions and validates path);
 
 I use a regex to split the data, because the BASE64 png data includes commas.
 
 I freely used online resources including:
 
 [w3schools](https://www.w3schools.com/)
 
 [geeksforgeeks](https://www.geeksforgeeks.org/)
 
 [stackoverflow](https://stackoverflow.com/)
 
 [sqlite.org](https://www.sqlite.org/)
 
 [sqlitetutorial.net](https://www.sqlitetutorial.net/)
 
## Improvements
 The table name could also be input in the same way as the database name; but incorporating SQL table name restrictions.
 