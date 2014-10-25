import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class CreateDB {
	
	/** The name of the MySQL account to use (or empty for anonymous) */
	private final String userName = "";

	/** The password for the MySQL account (or empty for anonymous) */
	private final String password = "";

	/** The name of the computer running MySQL */
	private final String serverName = "localhost";

	/** The port of the MySQL server (default is 3306) */
	private final int portNumber = 3306;

	/** The name of the database we are testing with (this default is installed with MySQL) */
	private final String dbName = "test";
	
	/** The name of the table we are testing with */
	private final String tableName = "gnutellatest";
	
	// Name of input text file 
	private final String textFile = "p2p-Gnutella04.txt";
	
	/**
	 * Get a new database connection
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", this.userName);
		connectionProps.put("password", this.password);

		conn = DriverManager.getConnection("jdbc:mysql://"
				+ this.serverName + ":" + this.portNumber + "/" + this.dbName,
				connectionProps);

		return conn;
	}
	
	/**
	 * Run a SQL command which does not return a recordset:
	 * CREATE/INSERT/UPDATE/DELETE/DROP/etc.
	 * 
	 * @throws SQLException If something goes wrong
	 */
	public boolean executeUpdate(Connection conn, String command) throws SQLException {
	    Statement stmt = null;
	    try {
	        stmt = conn.createStatement();
	        stmt.executeUpdate(command); // This will throw a SQLException if it fails
	        return true;
	    } finally {

	    	// This will run whether we throw an exception or not
	        if (stmt != null) { stmt.close(); }
	    }
	}
	
	public void run() {
		Connection conn = null;
		// Connect to MySQL
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
			return;
		}
		
		//Create a table
		try {
			String createString =
			        "CREATE TABLE " + this.tableName + " ( " +
			        "id INT NOT NULL AUTO_INCREMENT, " +
			        "node1 INT NOT NULL, " +
			        "node2 INT NOT NULL, PRIMARY KEY (id))";
			this.executeUpdate(conn, createString);
			System.out.println("Created a table");
	    } catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
		return;
		}
		
		// Read file into system
				try {
					
					FileInputStream fs= new FileInputStream(this.textFile);
					BufferedReader br = new BufferedReader(new InputStreamReader(fs));
					
					for(int i = 0; i < 4; ++i) {
					  br.readLine();
					}
					
					String line;
					String [] array = new String[2];
					int[] numarray = new int[2];
					
					while ((line = br.readLine()) != null) {
						
						//line = br.readLine();
						array = line.split("\\s");
						numarray[0] = Integer.parseInt(array[0]);
						numarray[1] = Integer.parseInt(array[1]);
						
						try {
						    String createString =
							        "INSERT INTO " + this.tableName + " (node1, node2) VALUES ( " +
							        numarray[0] + " , " +
							        numarray[1] + " )";
							this.executeUpdate(conn, createString);
							//System.out.println(Arrays.asList(array));
							//count = count + 1;
							//System.out.println(count);
							//System.out.println("Inserted row");
					    } catch (SQLException e) {
							System.out.println("ERROR: Could not insert row");
							e.printStackTrace();
							return;
						}
						
					}
					System.out.println("Done");
					br.close();	
				} catch (Exception e){
					e.printStackTrace();
				}
			
	}
	
	/**
	 * Connect to the DB and do some stuff
	 */
	public static void main(String[] args) {
		CreateDB app = new CreateDB();
		app.run();
	}
	
	
}
