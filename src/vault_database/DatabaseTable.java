package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DataType.InvalidDataTypeException;

/**
 * This class is an interface for all classes who need to represent tables in a database. 
 * DatabaseTables are used in multiTableHandling, for example. The subclasses should be 
 * enumerations rather than normal classes. In case a normal class inherits this class, it 
 * should override its equals method.
 * @author Mikko Hilpinen
 * @since 25.7.2014
 * @deprecated Use the {@link vault_generics.DatabaseTable} instead
 */
public interface DatabaseTable
{
	// ABSTRACT METHODS	----------------------------------------------------------
	
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
	 * @return The column that contains the entity's primary identifier. Null if 
	 * the table doesn't have a primary column (which is not recommended, by the way).
	 * @see #findPrimaryColumnInfo(Collection)
	 */
	public Column getPrimaryColumn();
	
	/**
	 * @return All the information about the columns in the table
	 */
	public List<Column> getColumnInfo();
	
	/**
	 * @return The column name to attribute name -mapping used with this table. The mapping 
	 * may be shared between instances or generated for each request, depending from the 
	 * subclasses implementation.
	 */
	public AttributeNameMapping getAttributeNameMapping();
	
	
	// OTHER METHODS	----------------------------
	
	/**
	 * Finds the primary column from the given set of columns
	 * @param columnSet A collection of columns
	 * @return The (first) primary column in the set
	 * @see #readColumnInfoFromDatabase(DatabaseTable)
	 */
	public static Column findPrimaryColumnInfo(Collection<Column> columnSet)
	{
		for (Column info : columnSet)
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
	public static List<String> getColumnNamesFromColumnInfo(List<Column> columnInfo)
	{
		List<String> columnNames = new ArrayList<>();
		for (Column info : columnInfo)
		{
			columnNames.add(info.getName());
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
	public static List<Column> readColumnInfoFromDatabase(DatabaseTable table) throws 
			DatabaseUnavailableException, SQLException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet result = null;
		List<Column> columnInfo = new ArrayList<>();
		
		try
		{
			statement = accessor.getPreparedStatement("DESCRIBE " + table.getTableName());
			result = statement.executeQuery();
			// Reads the field names
			while (result.next())
			{
				String name = result.getString("Field");
				boolean primary = "PRI".equalsIgnoreCase(result.getString("Key"));
				boolean autoInc = "auto_increment".equalsIgnoreCase(result.getString("Extra"));
				DataType type = new DataType(result.getString("Type"));
				boolean nullAllowed = "YES".equalsIgnoreCase(result.getString("Null"));
				Object defaultValue = null;
				if (!"NULL".equalsIgnoreCase(result.getString("Default")))
					defaultValue = result.getObject("Default");
				columnInfo.add(new Column(name, type, nullAllowed, primary, autoInc, 
						defaultValue));
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
	
	/**
	 * Searches for a column with the given name
	 * @param columnInfo A list of columns
	 * @param columnName The name of the searched column
	 * @return The column with the given name (not case-sensitive) or null if there isn't one
	 */
	public static Column findColumnWithName(Collection<? extends Column> columnInfo, 
			String columnName)
	{
		for (Column column : columnInfo)
		{
			if (column.getName().equalsIgnoreCase(columnName))
				return column;
		}
		
		return null;
	}
	
	/**
	 * Finds a column that is associated with the provided attribute name in the provided table
	 * @param table The table the column is in
	 * @param attributeName The name of the attribute the column should be associated with
	 * @return A column for the given attribute name
	 * @throws NoColumnForAttributeException If the attribute name couldn't be retraced 
	 * back to a column name
	 */
	public static Column findColumnForAttributeName(DatabaseTable table, 
			String attributeName) throws NoColumnForAttributeException
	{
		Column column = findColumnWithName(table.getColumnInfo(), 
				table.getAttributeNameMapping().getColumnName(attributeName));
		if (column == null)
			throw new NoColumnForAttributeException(attributeName);
		return column;
	}
	
	
	// SUBCLASSES	--------------------------
	
	/**
	 * ColumnInfo is a simple description of a single column in a table
	 * @author Mikko Hilpinen
	 * @since 30.5.2015
	 * @deprecated use vault_generics.Column instead
	 */
	public static class Column
	{
		// ATTRIBUTES	----------------------
		
		private String name;
		private boolean autoIncrement, primary, nullAllowed;
		private DataType type;
		private DatabaseValue defaultValue;
		
		
		// CONSTRUCTOR	----------------------
		
		/**
		 * Creates a new column information
		 * @param name The name of the column
		 * @param primary Does the column hold a primary key
		 * @param autoIncrement Does the column use auto-increment indexing
		 * @param type The data type of the column's value
		 * @param nullAllowed Is null allowed in this column
		 * @param defaultValue The default value used for this column
		 */
		public Column(String name, DataType type, boolean nullAllowed, boolean primary, 
				boolean autoIncrement, Object defaultValue)
		{
			this.name = name;
			this.primary = primary;
			this.autoIncrement = autoIncrement;
			this.type = type;
			this.nullAllowed = nullAllowed;
			
			try
			{
				this.defaultValue = DatabaseValue.objectValue(type, defaultValue);
			}
			catch (InvalidDataTypeException e)
			{
				// If default value can't be cast, it is ignored
				this.defaultValue = DatabaseValue.nullValue(type);
				System.err.println("Can't set default value for column " + name + " to " + 
						defaultValue);
			}
		}
		
		
		// IMPLEMENTED METHODS	---------------
		
		@Override
		public String toString()
		{
			StringBuilder s = new StringBuilder(getName());
			s.append(" (" + getType() + ")");
			if (isPrimary())
				s.append(" PRI");
			if (!nullAllowed())
				s.append(" not null");
			if (usesAutoIncrementIndexing())
				s.append(" auto-increment");
			if (getDefaultValue() != null)
				s.append(" default = " + getDefaultValue().toString());
			
			return s.toString();
		}
		
		
		// GETTERS & SETTERS	---------------
		
		/**
		 * @return The name of the column
		 */
		public String getName()
		{
			return this.name;
		}
		
		/**
		 * @return Does the column hold a primary key (default = false)
		 */
		public boolean isPrimary()
		{
			return this.primary;
		}
		
		/**
		 * @return Does the column use auto-increment indexing (default = false)
		 */
		public boolean usesAutoIncrementIndexing()
		{
			return this.autoIncrement;
		}
		
		/**
		 * @return The data type of the column value
		 */
		public DataType getType()
		{
			return this.type;
		}
		
		/**
		 * @return Is null value allowed in this column (default = true)
		 */
		public boolean nullAllowed()
		{
			return this.nullAllowed;
		}
		
		/**
		 * @return The default value used for this column (default = null)
		 */
		public DatabaseValue getDefaultValue()
		{
			return this.defaultValue;
		}
	}
}
