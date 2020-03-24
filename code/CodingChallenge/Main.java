
package code.CodingChallenge;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    public static Connection connect(String fileName) {
        // SQLite connection string
        String url = "jdbc:sqlite:" + System.getProperty("user.dir") + "\\" + fileName + ".db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public static void insert(Connection conn, String tableName, String[] data) {
        String sql = "INSERT INTO " + tableName + "(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";
        
        try (
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, data[0]);
            pstmt.setString(2, data[1]);
            pstmt.setString(3, data[2]);
            pstmt.setString(4, data[3]);
            pstmt.setBytes(5, data[4].getBytes(StandardCharsets.UTF_8));
            pstmt.setString(6, data[5]);
            if (data[6].length() > 1) {
                if (data[6].charAt(0) == '$') {
                    data[6] = data[6].substring(1, data[6].length());
                }
            } else {
                data[6] = "0";//default to zero
            }
            
            pstmt.setDouble(7, Double.parseDouble(data[6]));
            pstmt.setInt(8, stringBoolToInt(data[7]));
            pstmt.setInt(9, stringBoolToInt(data[8]));
            pstmt.setString(10, data[9]);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e);
            System.out.println(e.getMessage());
        }
    }

    public static int stringBoolToInt(String bool) {
        if (Boolean.parseBoolean(bool)) {
            return 1;
        } else {
            return 0;
        }
    }

    public static void createNewTable(Connection conn, String tableName) {
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
                + "    id integer PRIMARY KEY,\n"
                + "    A text NOT NULL,\n"
                + "    B text NOT NULL,\n"
                + "    C text NOT NULL,\n"
                + "    D text NOT NULL,\n"
                + "    E blob,\n"
                + "    F text NOT NULL,\n"
                + "    G real,\n"
                + "    H integer,\n"
                + "    I integer,\n"
                + "    J text NOT NULL\n"
                + ");";

        try {
            Statement stmt = conn.createStatement();
            // create a new table
            stmt.execute(sql);//.toString());
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createNewDatabase(String fileName) {

        String url = "jdbc:sqlite:" + System.getProperty("user.dir") + "\\" + fileName + ".db";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        String user_input;
        String dbFilename;
        String csvFilename = "";
        Boolean cont = false;
        BufferedReader csvReader = null;

        /*=========PROMPT USER FOR CSV PATH AND FILENAME=========*/
        
        while (cont == false) {
            cont = true;
            System.out.println("Please enter the path to your csv file (enter quit to exit):");
            user_input = scanner.nextLine();
            if (user_input.toLowerCase().equals("quit") || user_input.toLowerCase().equals("q")) {
                System.exit(0);
            }
            //strip the filename extension if entered
            String[] parseInput = user_input.split("\\.");
            if (parseInput.length > 1) {
                if(!parseInput[parseInput.length-1].contains("\\") && !parseInput[parseInput.length-1].contains("/")){
                    csvFilename = user_input.substring(0,user_input.lastIndexOf("."));
                }else{
                    csvFilename = user_input;
                }
            } else {
                csvFilename = user_input;
            }
            try {
                csvReader = new BufferedReader(new FileReader(System.getProperty("user.dir") + "\\" + csvFilename + ".csv"));

            } catch (FileNotFoundException e) {
                System.out.println(e.getMessage());
                cont = false;
            }
        }

        /*========PROMPT USER FOR DATABASE PATH AND FILENAME=========*/
        
        Connection conn = null;

        System.out.println("Use/Create database with the same path and file name? <Y/N>");
        user_input = scanner.nextLine();
        if (user_input.toLowerCase().equals("y")) {
            //Attempt to connect to a database with the same filename
            System.out.println("Connecting to database...");
            conn = connect(csvFilename);
            if(conn==null){
                System.out.println("Failed to connect to "+csvFilename +".db");
            }
        }

        while (conn == null) {
            System.out.println("Please enter the new database path (enter quit to exit):");
            user_input = scanner.nextLine();
            if (user_input.toLowerCase().equals("quit")) {
                System.exit(0);
            }
            //strip the filename extension if entered
            String[] parseInput = user_input.split("\\.");
            if (parseInput.length > 1) {
                if(!parseInput[parseInput.length-1].contains("\\") && !parseInput[parseInput.length-1].contains("/")){
                    dbFilename = user_input.substring(0,user_input.lastIndexOf("."));
                }else{
                    dbFilename = user_input;
                }
            } else {
                dbFilename = user_input;
            }
            conn = connect(dbFilename);
            if(conn==null){
                System.out.println("Failed to connect to "+dbFilename +".db");
            }
        }
        System.out.println("Verifying/Creating table...");
        createNewTable(conn, "table1");
        
        /*======READ CSV AND INSERT ROWS INTO TABLE======*/
        
        int badRows = 0;
        int rowNum = 0;
        

        ArrayList<String[]> rejectedRows = new ArrayList<>();
        
        try {
            double percentComplete;
            int barLength = 20;
            int numberOfLines = countLines(csvFilename + ".csv");
            int updateMod = 1 + (numberOfLines / barLength); //when we update the progress bar
            
            String row;
            boolean firstRow = true;

            System.out.println("Consuming CSV...");

            while ((row = csvReader.readLine()) != null) {

                String[] data = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                if (data.length > 10) {
                    badRows += 1;
                    rejectedRows.add(data);
                } else if (data.length < 10) {
                    //This will pick up the EOF line
                    badRows += 1;
                } else {
                    
                    if (!firstRow) {
                        insert(conn, "table1", data);
                    } else {
                        firstRow = false;
                    }
                }
                
                /*UPDATE PROGRESS BAR*/
                if (rowNum % updateMod == 0) {
                    StringBuilder barString = new StringBuilder();
                    barString.append("|");
                    percentComplete = barLength * rowNum / numberOfLines;
                    for (int i = 0; i < barLength; i++) {
                        if (i < percentComplete) {
                            barString.append("#");
                        } else {
                            barString.append(" ");
                        }
                    }
                    barString.append("| ~");
                    barString.append((int) 100 * rowNum / numberOfLines);
                    barString.append("%\r");
                    System.out.print(barString);
                }
                
                rowNum += 1;

            }
            System.out.print("Complete!");
            //print out enough spaces to overwrite the progress bar.
            System.out.format("%1$"+barLength+"s", "");
            System.out.println("");
            badRows--;//subtract the EOF line
            csvReader.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        
        /*===========WRITE REJECTED ROWS INTO -bad.csv=============*/ 
        
        try (FileWriter csvWriter = new FileWriter(csvFilename + "-bad.csv")) {
            for (String[] row : rejectedRows) {
                csvWriter.append(String.join(",", row));
                csvWriter.append("\n");
            }
            csvWriter.flush();
        }
        
        /*==================CREATE LOG FILE=======================*/
        try (FileWriter logfile = new FileWriter(csvFilename+".log")) {
            logfile.write(rowNum+" records processed\n");
            logfile.write(rowNum-badRows+" records succeeded\n");
            logfile.write(badRows+" records failed\n");
            System.out.println("Run information stored in logfile.");
        }catch(IOException e){
            System.out.println(e.getMessage());
            System.out.println("Unable to create logfile!");
        }
    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
}
