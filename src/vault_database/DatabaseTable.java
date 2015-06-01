package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
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
	public boolean usesIntegerIndexing();
	
	/**
	 * @return Does this table use indexing that uses auto-increment
	 * @see #readColumnInfoFromDatabase(DatabaseTable)
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
	 * @see #readColumnInfoFromDatabase(DatabaseTable)
	 */
	public List<String> getColumnNames();
	
	/**
	 * @return The name of the column that contains the entity's primary identifier. Null if 
	 * the table doesn't have a primary column.
	 * @see #findPrimaryColumnInfo(Collection)
	 */
	public String getPrimaryColumnName();
	
	
	// OTHER METHODS	----------------------------
	
	/**
	 * Finds the primary column from the given set of columns
	 * @param columnSet A collection of columns
	 * @return The (first) primary column in the set
	 * @see #readColumnInfoFromDatabase(DatabaseTable)
	 */
	public static ColumnInfo findPrimaryColumnInfo(Collection<ColumnInfo> columnSet)
	{
		for (ColumnInfo info : columnSet)
		{
			if (info.isPrimary())
				return info;
		}
		
		return null;
	}
	
	/**
	 * Finds the column names from the column information
	 * @param columnInfo The column information from a single table
	 * @return All the column names in a single table
	 */
	public static List<String> getColumnNamesFromColumnInfo(List<ColumnInfo> columnInfo)
	{
		List<String> columnNames = new ArrayList<>();
		for (ColumnInfo info : columnInfo)
		{
			columnNames.add(info.getColumnName());
		}
		
		return columnNames;
	}
	
	/**
	 * Reads the information of all the columns in the given table.
	 * @param table The table whose columns are checked
	 * @return A list containing the information about the columns in the table
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws SQLException If the table doesn't exist
	 */
	public static List<ColumnInfo> readColumnInfoFromDatabase(DatabaseTable table) throws 
			DatabaseUnavailableException, SQLException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		PreparedStatement statement = accessor.getPreparedStatement("DESCRIBE " + 
				DatabaseSettings.getTableHandler().getTableNameForIndex(table, 1, false));
		ResultSet result = null;
		List<ColumnInfo> columnInfo = new ArrayList<>();
		
		try
		{
			result = statement.executeQuery();
			// Reads the field names
			while (result.next())
			{
				String name = result.getString("Field");
				boolean primary = "PRI".equalsIgnoreCase(result.getString("Key"));
				boolean autoInc = "auto_increment".equalsIgnoreCase(result.getString("Extra"));
				columnInfo.add(new ColumnInfo(name, primary, autoInc));
			}
		}
		finally
		{
			// Closes the connections
			DatabaseAccessor.closeResults(result);
			DatabaseAccessor.closeStatement(statement);
			accessor.closeConnection();
		}
		
		return columnInfo;
	}
	
	
	// SUBCLASSES	--------------------------
	
	/**
	 * ColumnInfo is a simple description of a single column in a table
	 * @author Mikko Hilpinen
	 * @since 30.5.2015
	 */
	public static class ColumnInfo
	{
		// ATTRIBUTES	----------------------
		
		private String name;
		private boolean autoIncrement, primary;
		
		
		// CONSTRUCTOR	----------------------
		
		/**
		 * Creates a new column information
		 * @param name The name of the column
		 * @param primary Does the column hold a primary key
		 * @param autoIncrement Does the column use auto-increment indexing
		 */
		public ColumnInfo(String name, boolean primary, boolean autoIncrement)
		{
			this.name = name;
			this.primary = primary;
			this.autoIncrement = autoIncrement;
		}
		
		
		// IMPLEMENTED METHODS	---------------
		
		@Override
		public String toString()
		{
			String s = this.name;
			if (this.primary)
				s += " PRI";
			if (this.autoIncrement)
				s += " auto-increment";
			
			return s;
		}
		
		
		// GETTERS & SETTERS	---------------
		
		/**
		 * @return The name of the column
		 */
		public String getColumnName()
		{
			return this.name;
		}
		
		/**
		 * @return Does the column hold a primary key
		 */
		public boolean isPrimary()
		{
			return this.primary;
		}
		
		/**
		 * @return Does the column use auto-increment indexing
		 */
		public boolean usesAutoIncrementIndexing()
		{
			return this.autoIncrement;
		}
	}
}
