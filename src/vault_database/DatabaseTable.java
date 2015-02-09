package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is an interface for all classes who need to represent tables in a database. 
 * DatabaseTables are used in multiTableHandling, for example. The subclasses should be 
 * enumerations rather than normal classes. In case a normal class inherits this class, it 
 * should override its equals method.
 * 
 * @author Mikko Hilpinen
 * @since 25.7.2014
 */
public interface DatabaseTable
{
	// ABSTRACT METHODS	----------------------------------------------------------
	
	/**
	 * @return Is integer indexing used in this table
	 */
	public boolean usesIndexing();
	
	/**
	 * @return Does this table use indexing that uses auto-increment
	 */
	public boolean usesAutoIncrementIndexing();
	
	/**
	 * @return The name of the database that holds this table type
	 */
	public String getDatabaseName();
	
	/**
	 * @return The name of the table this ResourceTable represents
	 */
	public String getTableName();
	
	/**
	 * @return The names of the columns in this database
	 */
	public List<String> getColumnNames();
	
	
	// OTHER METHODS	----------------------------
	
	/**
	 * Reads the names of all the columns in the given table. This doesn't include the 
	 * auto-increment id, however.
	 * @param table The table whose columns are checked
	 * @return A list containing the names of the columns in the table
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws SQLException If the table doesn't exist
	 */
	public static List<String> readColumnNamesFromDatabase(DatabaseTable table) throws 
			DatabaseUnavailableException, SQLException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		PreparedStatement statement = accessor.getPreparedStatement("DESCRIBE " + 
				DatabaseSettings.getTableHandler().getTableNameForIndex(table, 1, false));
		ResultSet result = null;
		List<String> columnNames = new ArrayList<>();
		
		try
		{
			result = statement.executeQuery();
			// Reads the field names
			while (result.next())
			{
				columnNames.add(result.getString("Field"));
			}
		}
		finally
		{
			// Closes the connections
			DatabaseAccessor.closeResults(result);
			DatabaseAccessor.closeStatement(statement);
			accessor.closeConnection();
		}
		
		// Removes the id column (always first) in case there is (or should be) one
		if (table.usesAutoIncrementIndexing())
			columnNames.remove(0);
		
		return columnNames;
	}
	
	/**
	 * Combines two sets of possible database table values together. This can be used when 
	 * initializing the multiTableHandler, for example.
	 * @param a The first set of possible tables
	 * @param b The second set of possible tables
	 * @return A combined set of possible tables
	 */
	/*
	public static DatabaseTable[] combinePossibleTables(DatabaseTable[] a, DatabaseTable[] b)
	{
		DatabaseTable[] combined = new DatabaseTable[a.length + b.length];
		
		for (int i = 0; i < a.length; i++)
		{
			combined[i] = a[i];
		}
		
		for (int i = 0; i < b.length; i++)
		{
			combined[a.length + i] = b[i];
		}
		
		return combined;
	}
	*/
}
