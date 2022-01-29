import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

// Implement the meshJoin Algorithm
public class meshJoin {
	
    private static Integer numOfPartitions = 10; // 10 fixed partitions
    private static Integer numOfTransactions = 50; // Number of transactions to be fetched
    public static ArrayList<ArrayList<String>> S = new ArrayList<>(numOfPartitions); // Queue reading buffer
    public static Map<String, ArrayList<transactionData>> multiValueMap = new HashMap<String, ArrayList<transactionData>>(); // Hash map
    public static Integer masterDataRowsCount; // Store the number of rows in Master Data
    public static Integer transactionsRowCount; // Store the number of rows in the Transactions Data

    // Counts the number of rows in the Master Data
    public static Integer countMasterDataRows(Connection con) throws SQLException{
        String sql = "Select count(*) from masterdata;";
        PreparedStatement p = con.prepareStatement(sql);
        ResultSet rs = p.executeQuery();
        Integer numOfRows = 0;
        while(rs.next()){
            numOfRows = rs.getInt(1);
        }
        return numOfRows;
    }
    
    // Counts the number of rows in the Transactions Data
    public static Integer countTransactionDataRows(Connection con) throws SQLException{
        String sql = "Select count(*) from transactions;";
        PreparedStatement p = con.prepareStatement(sql);
        ResultSet rs = p.executeQuery();
        Integer numOfRows = 0;
        while(rs.next()){
            numOfRows = rs.getInt(1);
        }
        return numOfRows;
    }

    // Return the transactions in sets of 50. 
    public static Boolean fetchTransactions(Connection con, Integer value) throws SQLException {
        Statement stat = null;
        try {
            stat = con.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            //SQL Query to fetch transactions
            String sql = "Select * from Transactions Limit 50 offset ?;";
            PreparedStatement preparedStmt = con.prepareStatement(sql);
            preparedStmt.setInt(1, value);

            ResultSet rs = preparedStmt.executeQuery();
            Integer TRANSACTION_ID;
            String PRODUCT_ID = "";
            String CUSTOMER_ID = "";
            String CUSTOMER_NAME = "";
            String STORE_ID = "";
            String STORE_NAME = "";
            Date T_DATE;
            Integer QUANTITY;
            // JoinKeys - the list that contains all Product ID pointers in an element in the Queue
            ArrayList<String> JoinKeys = new ArrayList<>();
            while (rs.next()) {      //Get the Transactions from the transactions table
                TRANSACTION_ID = rs.getInt(1);
                PRODUCT_ID = rs.getString(2);
                CUSTOMER_ID = rs.getString(3);
                CUSTOMER_NAME = rs.getString(4);
                STORE_ID = rs.getString(5);
                STORE_NAME = rs.getString(6);
                T_DATE = rs.getDate(7);
                QUANTITY = rs.getInt(8);
                transactionData tData = new transactionData(TRANSACTION_ID, PRODUCT_ID, CUSTOMER_ID,
                        CUSTOMER_NAME, STORE_ID, STORE_NAME, T_DATE, QUANTITY);

                // Insert key if it does not already exist
                if (multiValueMap.get(PRODUCT_ID) == null) {
                    multiValueMap.put(PRODUCT_ID, new ArrayList<transactionData>());
                }
                // Add the new transaction into the multiValueMap
                multiValueMap.get(PRODUCT_ID).add(tData);

                // Add the respective Product IDs
                JoinKeys.add(PRODUCT_ID);
            }
            // Add to the Queue
            S.add(JoinKeys);
            return Boolean.TRUE;
        }
        catch (SQLException throwables){
            return Boolean.FALSE;
        }
    }
    
    // Fetches the Master Data in Chunks of size 10 each, returns a list of masterData objects
    public static ArrayList<masterData> fetchMasterData (Connection con, Integer offset) throws SQLException {
        Statement stat = null;
        try {
            stat = con.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        //SQL Query to fetch master data
        String sql = "Select * from masterdata Limit 10 offset ?;";
        PreparedStatement preparedStmt = con.prepareStatement(sql);
        preparedStmt.setInt(1, offset);
        ResultSet rs = preparedStmt.executeQuery();
        String PRODUCT_ID;
        String PRODUCT_NAME;
        String SUPPLIER_ID;
        String SUPPLIER_NAME;
        Float PRICE;
        ArrayList<masterData> MD = new ArrayList<masterData>(numOfPartitions);
        while (rs.next()) {
            PRODUCT_ID =rs.getString(1);
            PRODUCT_NAME = rs.getString(2);
            SUPPLIER_ID = rs.getString(3);
            SUPPLIER_NAME =rs.getString(4);
            PRICE =rs.getFloat(5);
            masterData r = new masterData(PRODUCT_ID,PRODUCT_NAME,SUPPLIER_ID,SUPPLIER_NAME,PRICE);
            MD.add(r);
        }
        return MD;
    }

    // Update the contents of the Hash map to the DWH
    public static void updateToDWH(Connection con, transactionData tD) throws SQLException {
        //UPDATING DIMENSION TABLES
        // Search if the Store ID exists in the DWH
        String sql_store = "Select * from Store_Dim where StoreID = ? ;";
        PreparedStatement preparedStmt = con.prepareStatement(sql_store);
        preparedStmt.setString(1, tD.STORE_ID);
        ResultSet rs = preparedStmt.executeQuery();

        // Not found (Store ID)
        if (!rs.next()){
            String insert_store = "Insert into Store_Dim Values(?,?)";
            PreparedStatement ps = con.prepareStatement(insert_store);
            ps.setString(1,tD.STORE_ID);
            ps.setString(2,tD.STORE_NAME);
            ps.execute();
        }

        // Search if the Customer ID exists in the DWH
        String sql_customer = "Select * from Customer_Dim where CustomerID = ?";
        PreparedStatement preparedStmt2 = con.prepareStatement(sql_customer);
        preparedStmt2.setString(1, tD.CUSTOMER_ID);
        ResultSet rs2 = preparedStmt2.executeQuery();
        if (!rs2.next()){
            String insert_customer = "Insert into Customer_Dim Values(?,?);";
            PreparedStatement ps2 = con.prepareStatement(insert_customer);
            ps2.setString(1, tD.CUSTOMER_ID);
            ps2.setString(2,tD.CUSTOMER_NAME);
            ps2.execute();
        }

        // Search if the Supplier ID exists in the DWH
        String sql_supplier = "Select * from Supplier_Dim where SupplierID = ?";
        PreparedStatement preparedStmt3 = con.prepareStatement(sql_supplier);
        preparedStmt3.setString(1, tD.SUPPLIER_ID);
        ResultSet rs3 = preparedStmt3.executeQuery();
        if (!rs3.next()){
            String insert_supplier = "Insert into Supplier_Dim Values(?,?);";
            PreparedStatement ps3 = con.prepareStatement(insert_supplier);
            ps3.setString(1, tD.SUPPLIER_ID);
            ps3.setString(2,tD.SUPPLIER_NAME);
            ps3.execute();
        }

        // Search if the Supplier ID exists in the DWH
        String sql_product = "Select * from Product_Dim where ProductID = ?";
        PreparedStatement preparedStmt4 = con.prepareStatement(sql_product);
        preparedStmt4.setString(1, tD.PRODUCT_ID);
        ResultSet rs4 = preparedStmt4.executeQuery();
        if (!rs4.next()){
            String insert_product = "Insert into Product_Dim Values(?,?);";
            PreparedStatement ps4 = con.prepareStatement(insert_product);
            ps4.setString(1, tD.PRODUCT_ID);
            ps4.setString(2,tD.PRODUCT_NAME);
            ps4.execute();
        }

        // Search if Date exists in the DWH
        String sql_date = "Select * from Date_Dim where DateID = ?";
        PreparedStatement preparedStmt5 = con.prepareStatement(sql_date);
        int month = tD.T_DATE.getMonth() + 1;
        // Find the quarter
        int quarter;
        if (month <=3){
            quarter = 1;
        }
        else if(month>3 && month <=6){
            quarter = 2;
        }
        else if(month>6 && month<=9){
            quarter = 3;
        }
        else{
            quarter = 4;
        }
        // Convert java util date to Java SQL Date
        java.sql.Date sDate = new java.sql.Date(tD.T_DATE.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(sDate);
        int year = cal.get(Calendar.YEAR);
        // Find the day of the week
        String dayWeek = new SimpleDateFormat("EEEE").format(sDate);
        preparedStmt5.setDate(1, sDate);

        ResultSet rs5 = preparedStmt5.executeQuery();
        if (!rs5.next()){
            String insert_date = "Insert into Date_Dim Values(?,?,?,?,?);";
            PreparedStatement ps5 = con.prepareStatement(insert_date);
            ps5.setDate(1, sDate);
            ps5.setString(2, dayWeek);
            ps5.setInt(3, month);
            ps5.setInt(4, quarter);
            ps5.setInt(5, year);

            ps5.execute();
        }

        // INSERT INTO FACT TABLE
        String sql_trans = "INSERT into Transactions_Fact VALUES(?,?,?,?,?,?,?,?);";
        PreparedStatement ps6 = con.prepareStatement(sql_trans);
        ps6.setInt(1,tD.TRANSACTION_ID);
        ps6.setString(2, tD.STORE_ID);
        ps6.setString(3, tD.PRODUCT_ID);
        ps6.setString(4, tD.SUPPLIER_ID);
        ps6.setDate(5,sDate);
        ps6.setString(6, tD.CUSTOMER_ID);
        ps6.setFloat(7, tD.TOTAL_SALE);
        ps6.setInt(8, tD.QUANTITY);
        ps6.execute();

    }

    // Search in the Hash Table for  product ID that matches the Master Data's Product ID
    public static void lookUpHashTable(Connection con, ArrayList<masterData> MD) throws SQLException {
        // Getting an iterator
        Iterator hashMapIter = multiValueMap.entrySet().iterator();

        while(hashMapIter.hasNext()){
            Map.Entry tupleListEntry = (Map.Entry)hashMapIter.next();
            // Get the key of the hashMap
            String product_id = (String)tupleListEntry.getKey();
            // Iterate over the master data in the disk buffer
            for(masterData m : MD){
                // Can enrich the data
                if (m.PRODUCT_ID.equals(product_id)){
                    ArrayList<transactionData> tupleList = (ArrayList<transactionData>)tupleListEntry.getValue();
                    // Copy of the tuple list
                    ArrayList<transactionData> tupleListNew = new ArrayList<transactionData>(tupleList);
                    for(transactionData tD: tupleList ){
                        int index = tupleList.indexOf(tD);
                        tD.PRODUCT_NAME = m.PRODUCT_NAME;
                        tD.SUPPLIER_ID = m.SUPPLIER_ID;
                        tD.SUPPLIER_NAME = m.SUPPLIER_NAME;
                        tD.TOTAL_SALE = tD.QUANTITY * m.PRICE;
                        // Update the tuple
                        tupleListNew.set(index, tD);
                        //Update to DWH
                        updateToDWH(con, tD);

                    }
                    // Update the hash map
                    multiValueMap.put(product_id, tupleListNew);
                }
            }
        }
    }
    
    // Remove keys/values from the Hash map and the Queue
    public static void removeFromHashMapAndQueue(){
    	// Remove the Queue head
        ArrayList<String> head = S.remove(0);
        for(String s : head){
            Iterator hashMapIter = multiValueMap.entrySet().iterator();

            while(hashMapIter.hasNext()){
                Map.Entry tupleListEntry = (Map.Entry)hashMapIter.next();
                // Get the key of the hashMap
                String product_id = (String)tupleListEntry.getKey();
                // Remove respective tuples that have been removed from queue
                if (product_id.equals(s)) {
                    ArrayList<transactionData> tupleList = (ArrayList<transactionData>) tupleListEntry.getValue();
                    // Create a copy of the tupleList
                    ArrayList<transactionData> tupleListNew = new ArrayList<transactionData>(tupleList);
                    for (transactionData tD : tupleList) {

                        // Only Delete if all the values are set
                        if (tD.TRANSACTION_ID != null && tD.PRODUCT_ID != null && tD.CUSTOMER_ID != null && tD.CUSTOMER_NAME != null &&
                                tD.STORE_ID != null && tD.STORE_NAME != null && tD.T_DATE != null && tD.QUANTITY != null &&
                                tD.SUPPLIER_ID != null && tD.SUPPLIER_NAME != null && tD.TOTAL_SALE != null) {
                            tupleListNew.remove(tD);
                        }
                    }
                    multiValueMap.put(product_id, tupleListNew);
                }
            }

        }

    }

    // main function
    public static void main(String[] args) throws SQLException {
    	// Get DB instance
        DBConnect db = DBConnect.getInstance();
        masterDataRowsCount = countMasterDataRows(db.con); //count rows in master data
        transactionsRowCount = countTransactionDataRows(db.con);// count rows in transactions data
        Integer value = 0;
        Integer diskBuffer = 0;
        Integer removeEveryTime = 0;
        Integer x = 0;
        // Loop while the Queue is not empty
        while(!S.isEmpty() || x==0){
            x+=1;
            if(value <= transactionsRowCount) {
                // Fetch the 50 tuples from Transaction table
                Boolean allFetched = fetchTransactions(db.con, value);
                value += numOfTransactions;
            }

            // Load the MD into diskBuffer
            ArrayList<masterData> MD = fetchMasterData(db.con, diskBuffer);
            diskBuffer += numOfPartitions;
            // Search for records that match
            lookUpHashTable(db.con, MD);

            // If all the master data partitions have been read, remove hash table entries

            if (diskBuffer == masterDataRowsCount || removeEveryTime == 1){
                removeEveryTime = 1;
                if (diskBuffer == masterDataRowsCount){
                    diskBuffer = 0;
                }

                // Delete functionality
                removeFromHashMapAndQueue();
            }
            if (value%1000 == 0) {
            	System.out.println("Please wait... Inserted: " + value );
            }
      }
        System.out.println("Updated all records to the DWH\n");
        
    }

}

