package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * MultiTableHandler makes possible to divide a big chunk of data into multiple tables. 
 * The handler keeps track of these tables and helps other objects access them.
 * 
 * @author Mikko Hilpinen
 * @since 25.7.2014
 */
public class MultiTableHandler
{
	// ATTRIBUTES	--------------------------------------------------------------------
	
	private final int MAXROWCOUNT;
	private final String DBNAME;
	private final String INFOTABLENAME;
	
	// How many instances there are of each table
	private HashMap<DatabaseTable, Integer> tableAmounts;
	private DatabaseTable[] possibleTables;
	
	
	// CONSTRUCTOR	--------------------------------------------------------------------
	
	/**
	 * Creates a new MultiTableHandler. The handler's status is retrieved from a database.
	 * @param possibleTables All the possible tables that could be handled by this handler
	 * @param maxRowCount How many rows there should be in a single table at maximum
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public MultiTableHandler(DatabaseTable[] possibleTables, int maxRowCount) 
			throws DatabaseUnavailableException, SQLException
	{
		// Initializes attributes
		this.MAXROWCOUNT = maxRowCount;
		this.DBNAME = "multitable_db";
		this.INFOTABLENAME = "tableamounts";
		
		this.tableAmounts = new HashMap<DatabaseTable, Integer>();
		this.possibleTables = possibleTables;
		
		// Loads / initializes the data from the database
		loadData();
	}
	
	/**
	 * Creates a new MultiTableHandler. The handler's status is retrieved from a database.
	 * @param possibleTables All the possible tables that could be handled by this handler
	 * @param maxRowCount How many rows there should be in a single table at maximum
	 * @param databaseName The name of the database that holds the multitable data
	 * @param tableName The name of the table that holds the multitable data
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public MultiTableHandler(DatabaseTable[] possibleTables, int maxRowCount, 
			String databaseName, String tableName) throws DatabaseUnavailableException, 
			SQLException
	{
		// Initializes attributes
		this.MAXROWCOUNT = maxRowCount;
		this.DBNAME = databaseName;
		this.INFOTABLENAME = tableName;
		
		this.tableAmounts = new HashMap<DatabaseTable, Integer>();
		this.possibleTables = possibleTables;
		
		// Loads / initializes the data from the database
		loadData();
	}
	
	
	// GETTERS & SETTERS	-------------------------------------------------------------
	
	/**
	 * Returns how many tables of the given type are currently in use
	 * 
	 * @param table a table that needs to be counted
	 * @return How many tables of that type there currently are
	 */
	public int getTableAmount(DatabaseTable table)
	{
		if (table == null)
			return 0;
		
		if (!table.usesIndexing())
			return 1;
		
		// TODO: Nullpointer?
		return this.tableAmounts.get(table);
	}
	
	
	// OTHER METHODS	-----------------------------------------------------------------
	
	/**
	 * Returns the name of the table that should contain the given id
	 * 
	 * @param table The type of the table requested
	 * @param id The unique identifier that may be found in a table
	 * @param createIfNecessary Should new tables be created if the given table doesn't exist yet?
	 * @return What name a table has that contains the given identifier
	 */
	public String getTableNameForIndex(DatabaseTable table, int id, boolean createIfNecessary)
	{
		// The system is a bit different for tables that don't use indexing
		if (!table.usesIndexing())
			return table.getTableName() + "1"; 
		
		// Creates new tables until one can be found for the given id
		if (createIfNecessary)
		{
			try
			{
				while (getTableNumberForID(id) > getTableAmount(table))
				{
					increaseTableAmount(table);
				}
			}
			catch (SQLException | DatabaseUnavailableException e)
			{
				System.err.println("Failed to increase the table amount");
				e.printStackTrace();
			}
		}
		
		return table.getTableName() + getTableNumberForID(id);
	}
	
	/**
	 * The name of the most recent table of the given type
	 * 
	 * @param table The type of the table whose name is requested
	 * @return The name of the most recent table of the given type
	 */
	public String getLatestTableName(DatabaseTable table)
	{
		return table.getTableName() + getTableAmount(table);
	}
	
	/**
	 * Informs the MultiTableHandler that a new row was added to a table. The MultiTableHandler 
	 * requires this information for the system to work.
	 * 
	 * @param table The table type that was updated
	 * @param newID The index of the new addition (if applicable)
	 */
	public void informAboutNewRow(DatabaseTable table, int newID)
	{
		// If the table doesn't use indexing, doesn't do anything
		if (!table.usesIndexing())
			return;
		
		// Also checks that the index would actually create a new table and creates a new table if necessary
		if (getTableNumberForID(newID + 1) > getTableAmount(table))
		{
			try
			{
				increaseTableAmount(table);
			}
			catch (SQLException | DatabaseUnavailableException e)
			{
				System.err.println("Failed to update the table amounts");
				e.printStackTrace();
			}
		}
	}
	
	private void saveStateIntoDatabase(DatabaseTable updatedTable) throws SQLException, 
			DatabaseUnavailableException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(this.DBNAME);
		
		accessor.executeStatement("UPDATE " + this.INFOTABLENAME + " SET latestIndex = " + 
				getTableAmount(updatedTable) + " WHERE tableName = '" + 
				updatedTable.getTableName() + "'");
		
		accessor.closeConnection();
	}
	
	private void loadData() throws DatabaseUnavailableException, SQLException
	{
		// If there already is data, doesn't load
		if (!this.tableAmounts.isEmpty())
			return;
		
		DatabaseAccessor accessor = new DatabaseAccessor(this.DBNAME);
		
		PreparedStatement statement = null;
		ResultSet results = null;
		
		try
		{
			statement = accessor.getPreparedStatement("SELECT * from " + this.INFOTABLENAME);
			results = statement.executeQuery();
			
			while (results.next())
			{
				String tableName = results.getString("tableName");
				int latestIndex = results.getInt("latestIndex");
				this.tableAmounts.put(parseTableFromString(tableName), latestIndex);
			}
		}
		finally
		{
			DatabaseAccessor.closeResults(results);
			DatabaseAccessor.closeStatement(statement);
		}
		
		// If no data was read, initializes the data manually and also records it to the database
		if (this.tableAmounts.isEmpty())
			initializeDataAsNew(accessor);
		
		accessor.closeConnection();
	}
	
	private void initializeDataAsNew(DatabaseAccessor accessor) throws SQLException, 
			DatabaseUnavailableException
	{
		// Initializes the object's local data
		for (DatabaseTable table : this.possibleTables)
		{
			if (!table.usesIndexing())
				continue;
			
			this.tableAmounts.put(table, 1);
		}
		
		// Removes the existing data
		accessor.executeStatement("DELETE FROM " + this.INFOTABLENAME);
		
		// Inserts the new data
		for (DatabaseTable table : this.tableAmounts.keySet())
		{
			accessor.executeStatement("INSERT INTO " + this.INFOTABLENAME + " VALUES ('" + 
					table.getTableName() + "', " + this.tableAmounts.get(table) + ")");
		}
	}
	
	private DatabaseTable parseTableFromString(String tableName)
	{
		for (DatabaseTable table : this.possibleTables)
		{
			if (table.getTableName().equalsIgnoreCase(tableName))
				return table;
		}
		
		return null;
	}
	
	private int getTableNumberForID(int id)
	{
		int tableNumber = id / this.MAXROWCOUNT + 1;
		
		// Indexes at max row count are still in the previous table
		if (id % this.MAXROWCOUNT == 0)
			tableNumber --;
		
		return tableNumber;
	}
	
	// Creates a new empty table like the previous one, the new table uses different auto_indexing
	private void increaseTableAmount(DatabaseTable table) throws SQLException, DatabaseUnavailableException
	{
		String oldTableName = getLatestTableName(table);
		
		this.tableAmounts.put(table, getTableAmount(table) + 1);
		saveStateIntoDatabase(table);
		
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		try
		{
			String newTableName = getLatestTableName(table);
			
			// Copies the previous table
			accessor.executeStatement("CREATE TABLE " + newTableName + " LIKE " + oldTableName);
			
			// Changes the auto_increment start value (if possible)
			if (table.usesAutoIncrementIndexing())
			{
				int newIncrement = (getTableAmount(table) - 1) * this.MAXROWCOUNT + 1;
				accessor.executeStatement("ALTER TABLE " + newTableName + " AUTO_INCREMENT = " + 
						newIncrement);
			}
		}
		finally
		{
			// Closes the connection
			accessor.closeConnection();
		}
	}
}
