import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
// DATABASE CONNECTION
public class DBConnect {    // Connect to the Database

    private static DBConnect DBConnection=null;

    public Connection con =null;
    private DBConnect() throws SQLException {
        try{
            String host = "jdbc:mysql://localhost:3306/dwhproject";
            String Name = "root";
            String Pass = "1234";
            con = DriverManager.getConnection(host, Name, Pass);
        }
        catch (SQLException err) {
            System.out.println(err.getMessage());
        }
    }
    public static DBConnect getInstance() throws SQLException {
        if(DBConnection==null)
        {
            DBConnection=new DBConnect();
        }
        return DBConnection;
    }
}
