import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

public class NetworkAnalysis {

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
	private final String tableName = "gnutella";
	
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
	
	// Get number of neighbours for given node
	public int NeighbourCount (int node){
		
		// Connect to MySQL
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
		}	
		
		int numberOfNeighbours = 0;
		
		String neighbours = "SELECT count(*) AS count FROM " + this.tableName + 
				" WHERE node1 = ?";
		
		try {
			PreparedStatement statement = conn.prepareStatement(neighbours);
			statement.setInt(1, node);
	
			ResultSet rs = statement.executeQuery();
			while (rs.next()){
				numberOfNeighbours = rs.getInt("count");
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		
		try{
			conn.close();
		} catch(Exception e){
			e.printStackTrace();
		}
		return numberOfNeighbours;
	}
	
	// Get reachability count (all neighbours) of given node by 
	public int ReachabilityCount(int node){
		
		// Connect to MySQL
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
		}
		
		LinkedList<Integer> nodesToQuery = new LinkedList<Integer>();
		Set<Integer> neighboursOfNodes = new HashSet<Integer>();
		Set<Integer> toBeInserted = new HashSet<Integer>();
		int reachableCount = 0;
		int queryCount = 0;
		// Create temporary table to store temporary values
		try {
		    String createString = "CREATE TABLE nodesreachable (id INT NOT NULL AUTO_INCREMENT, " +
			        "nodes INT NOT NULL, PRIMARY KEY (id))";
			this.executeUpdate(conn, createString);
			System.out.println("Created a table");
	    } catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
		}
		
		nodesToQuery.add(node);
		toBeInserted.add(node);
		
		// Insert initial node into 'nodesreachable' table
		reachableCount = insertIntoReachableTable (toBeInserted, reachableCount);
		toBeInserted.clear();
		
		while (!nodesToQuery.isEmpty()){
			
			// Select all neighbours of nodes in the nodesToQuery LinkedList
			neighboursOfNodes = getNeighboursOfNodes(nodesToQuery);
			nodesToQuery.clear();
			
			// Insert neighbours of nodes to reachablenodes table and nodestoquery list IF 
			// they do not already exist in the reachablenodes table
			
			Iterator<Integer> i = neighboursOfNodes.iterator();
			while (i.hasNext()){
				int next = i.next();
				if (!containsNode("nodesreachable",next)){
					toBeInserted.add(next);
					nodesToQuery.add(next);
				}
			}
			
			// Insert new reachable nodes into nodesreachable table
			if (toBeInserted.size() > 0){
				reachableCount = insertIntoReachableTable(toBeInserted, reachableCount);
				toBeInserted.clear();
			}
			
			queryCount = queryCount + 1;
			//System.out.println(queryCount);
		}
				
		// Drop temporary table
		try {
		    String dropString = "DROP TABLE nodesreachable";
			this.executeUpdate(conn, dropString);
			System.out.println("Dropped the table");
	    } catch (SQLException e) {
			System.out.println("ERROR: Could not drop the table");
			e.printStackTrace();
		}
		
		try{
			conn.close();
		} catch(Exception e){
			e.printStackTrace();
		}
		
		//System.out.println("Query Count: "+ queryCount);
		return reachableCount - 1;
	}
	
	public Set<Integer> getNeighboursOfNodes (LinkedList<Integer> nodesToQuery){
		Set<Integer> neighboursOfNodes = new HashSet<Integer>();
		String selectString = "SELECT node2 FROM " + this.tableName + " WHERE node1 IN";
		selectString = completeSelectStatement(selectString, nodesToQuery.size());
		
		try{
			PreparedStatement statement = conn.prepareStatement(selectString);
			setParametersForQuery(statement, nodesToQuery);
			ResultSet rs = statement.executeQuery();
			
			while (rs.next()){
				neighboursOfNodes.add(rs.getInt("node2"));
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
		
		return neighboursOfNodes;
	}
	
	public int insertIntoReachableTable (Set<Integer> toBeInserted, int reachableCount){
		String insertString = "INSERT INTO nodesreachable (nodes) VALUES ";
		insertString = completeInsertStatement(insertString,toBeInserted.size());
		
		try{
			PreparedStatement statement = conn.prepareStatement(insertString);
			setParametersForInsert(statement,toBeInserted);
			statement.executeUpdate();
			
		}catch (Exception e){
			e.printStackTrace();
		}
		
		reachableCount = reachableCount + toBeInserted.size();
		return reachableCount;
	}
	
	static String completeSelectStatement(String statementStr, int count) {
        StringBuilder result = new StringBuilder(statementStr);
        result.append(" (");
        for (int i = 0; i < count; i++) {
            result.append("?");
            if (i != count - 1) {
                result.append(",");
            }
        }
        result.append(")");
        return result.toString();
    }
	
	static String completeInsertStatement(String statementStr, int count) {
        StringBuilder result = new StringBuilder(statementStr);
        //result.append(" (");
        for (int i = 0; i < count; i++) {
            result.append("(?");
            if (i != count - 1) {
                result.append("),");
            }
        }
        result.append(")");
        //System.out.println(result.toString());
        return result.toString();
    }

    static void setParametersForQuery(PreparedStatement stmt, LinkedList<Integer> nodeValues) throws SQLException {
        int len = nodeValues.size();
        int node;
        
        for (int i = 1;  i <= len; i++) {
            node = nodeValues.pop();
            stmt.setInt(i, node);
        }
    }
    
    static void setParametersForInsert(PreparedStatement stmt, Set<Integer> nodeValues) throws SQLException {
       
        int count = 1;
        // Parameters are 1-indexed (arg)
        Iterator<Integer> i = nodeValues.iterator();
        while (i.hasNext()){
        	stmt.setInt(count, i.next());
        	count = count + 1;
        }
    }
	
	// Method to find either 3-cliques or 4-cliques
	public Set<Integer[]> DiscoverCliques (int k){
		
		// Connect to MySQL
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
		}	
		
		// Initialize variables and data structures
		Set<Integer[]> cliqueGroups = new HashSet<Integer[]>();
		Set<Integer> nonCandidates = new HashSet<Integer>();
		Set<Integer> commonNeighbours = new HashSet<Integer>();
		Set<Integer> node1Neighbours = new HashSet<Integer>();
		Set<Integer> node2Neighbours = new HashSet<Integer>();
		ArrayList<Integer> commonNeighboursList = new ArrayList<Integer>();
		int[] row = new int[2];
		int node1 = 0;
		int node2 = 0;
		int idcount = 1;
		
		row = getRow(idcount);
		
		// Iterate through every row (naive brute force implementation)
		while (row[0] != -1) {
			
			node1 = row[0];
			node2 = row[1];
			
			if (nonCandidates.contains(node1) || nonCandidates.contains(node2)){
				idcount = idcount + 1;
				row = getRow(idcount);
				continue;
			}
			
			// Get neighbours of node1 and node2 in current row
			node1Neighbours = getNeighbours(node1);
			node2Neighbours = getNeighbours(node2);
			
			// Check to see that each node has enough neighbours
			if (node1Neighbours.size() < k-1) {
				nonCandidates.add(node1);
				idcount = idcount + 1;
				row = getRow(idcount);
				continue;
			}
			else if (node2Neighbours.size() < k-1){
				nonCandidates.add(node2);
				idcount = idcount + 1;
				row = getRow(idcount);
				continue;
			}
			
			// Find common neighbours between 2 sets
			commonNeighbours = node1Neighbours;
			commonNeighbours.retainAll(node2Neighbours);
			
			if (commonNeighbours.size() == 0) {
				idcount = idcount + 1;
				row = getRow(idcount);
				continue;
			}
			else if (commonNeighbours.size() < k - 2){
				idcount = idcount + 1;
				row = getRow(idcount);
				continue;
			}
			else {
				if (k == 3){
					Iterator<Integer> i = commonNeighbours.iterator();
					while (i.hasNext()){
						Integer[] clique = new Integer[]{node1,node2,i.next()};
						Arrays.sort(clique);
						cliqueGroups.add(clique);
						//System.out.println(clique[0]+","+clique[1]+","+clique[2]);		
					}
				}
				else if (k == 4){
					Iterator<Integer> i = commonNeighbours.iterator();
					while (i.hasNext()){
						commonNeighboursList.add(i.next());
					}
					
					for (int p = 0; p < commonNeighboursList.size() - 1; p ++){
						Set<Integer> newNeighbours = getNeighbours(commonNeighboursList.get(p));
						for (int q = p + 1; q <commonNeighboursList.size(); q++){
							if (newNeighbours.contains(commonNeighboursList.get(q))){
								Integer[] clique = new Integer[]{node1,node2,commonNeighboursList.get(p),commonNeighboursList.get(q)};
								Arrays.sort(clique);
								cliqueGroups.add(clique);
								//System.out.println(clique[0]+","+clique[1]+","+clique[2]+","+clique[3]);
							}
						}
					}
				}
			}
			
			idcount = idcount + 1;
			System.out.println(idcount);
			row = getRow(idcount);
		}
		
		try{
			conn.close();
		} catch(Exception e){
			e.printStackTrace();
		}
		
		//System.out.println("Done");
		return cliqueGroups;
	}
	
	public boolean containsNode (String tableName, int i){
		try{
			
			String selectQuery = "SELECT nodes FROM " + tableName + " WHERE nodes = ?";
			PreparedStatement statement = conn.prepareStatement(selectQuery);
			statement.setInt(1, i);
	
			ResultSet rs = statement.executeQuery();
			
			if (!rs.next()) {
			    return false;  
			} else {
				return true;
			}
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
	}
	
	
	public int[] getRow (int id){
		int[] row = new int[]{-1, -1};
		try{
			
			String selectQuery = "SELECT node1,node2 FROM " + this.tableName + " WHERE id = ?";
			PreparedStatement statement = conn.prepareStatement(selectQuery);
			statement.setInt(1, id);
	
			ResultSet rs = statement.executeQuery();

				while (rs.next()){
					row[0] = rs.getInt("node1");
					row[1] = rs.getInt("node2");
					return row;
				} 
			 
		} catch (Exception e){
			e.printStackTrace();
		}
		System.out.println(row[0]);
		return row;
	}
	
	public Set<Integer> getNeighbours (int node){
		Set<Integer> neighbours = new HashSet<Integer>();
		
		try{
			
			String selectQuery = "SELECT node1,node2 FROM " + this.tableName + " WHERE node1 = ?" +
					" OR node2 = ?";
			PreparedStatement statement = conn.prepareStatement(selectQuery);
			statement.setInt(1, node);
			statement.setInt(2, node);
	
			ResultSet rs = statement.executeQuery();

			while (rs.next()){
				if (rs.getInt("node1") == node){
					neighbours.add(rs.getInt("node2"));
				}
				else {
					neighbours.add(rs.getInt("node1"));
				}
			} 
			 
		} catch (Exception e){
			e.printStackTrace();
		}
		/*Iterator i = neighbours.iterator();
		while (i.hasNext()){
			System.out.println(i.next());
		}*/
		return neighbours;
	}
	
	// Make given database undirected by removing any inverse entries. Method currently specific
	// to given database (it is not general)
	public void makeDatabaseUndirected (Connection conn, String tableName) {
		int idcounter = 1;
		int node1 = 0;
		int node2 = 0;
		int flag = 0;
		
		while (true) {
			try{
				String selectQuery = "SELECT node1,node2 FROM " + tableName + " WHERE id = ?";
				PreparedStatement statement = conn.prepareStatement(selectQuery);
				//statement.setString(1, tableName);
				statement.setInt(1, idcounter);
				//System.out.println("Set");
				ResultSet rs = statement.executeQuery();
	
				if (!rs.next()){
					flag = 1;
					 if (idcounter > 100000){
						 break;
					 }
				} else {
					node1 = rs.getInt("node1");
					node2 = rs.getInt("node2");
				}
				
			} catch (Exception e){
				e.printStackTrace();
			}
			
			if (flag == 0){
				try {
					String deleteQuery = "DELETE FROM " + tableName + " WHERE node1 = " +
							node2 + " AND node2 = " + node1;
							this.executeUpdate(conn,deleteQuery);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("No such combo");
				}
			} else {
				flag = 0;
			}
			
			idcounter = idcounter + 1;
		}
		System.out.println("ID counter is: " + idcounter);
	}
	
	Connection conn = null;
}
