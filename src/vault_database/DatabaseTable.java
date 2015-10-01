package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_database.AttributeNameMapping.NoColumnForAttributeException;

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
	public ColumnInfo getPrimaryColumn();
	
	/**
	 * @return All the information about the columns in the table
	 */
	public List<ColumnInfo> getColumnInfo();
	
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
	public static List<ColumnInfo> readColumnInfoFromDatabase(DatabaseTable table) throws 
			DatabaseUnavailableException, SQLException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet result = null;
		List<ColumnInfo> columnInfo = new ArrayList<>();
		
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
				int type = parseType(result.getString("Type"));
				boolean nullAllowed = "YES".equalsIgnoreCase(result.getString("Null"));
				Object defaultValue = null;
				if (!"NULL".equalsIgnoreCase(result.getString("Default")))
					defaultValue = result.getObject("Default");
				columnInfo.add(new ColumnInfo(name, type, nullAllowed, primary, autoInc, defaultValue));
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
	 * Parses an sql type from a string
	 * @param s A string that represents a data type. For example "varchar(32)"
	 * @return The data type represented by the string
	 * @see Types
	 */
	public static int parseType(String s)
	{
		String lower = s.toLowerCase();
		if (lower.startsWith("tiny"))
			return parseType(s.substring(4));
		if (lower.startsWith("big"))
			return parseType(s.substring(3));
		if (lower.startsWith("medium"))
			return parseType(s.substring(6));
		if (lower.startsWith("small"))
			return parseType(s.substring(5));
		if (lower.startsWith("varchar"))
			return Types.VARCHAR;
		if (lower.startsWith("int"))
			return Types.INTEGER;
		if (lower.startsWith("boolean"))
			return Types.BOOLEAN;
		if (lower.startsWith("float"))
			return Types.FLOAT;
		if (lower.startsWith("decimal"))
			return Types.DECIMAL;
		if (lower.startsWith("double"))
			return Types.DOUBLE;
		if (lower.startsWith("char"))
			return Types.CHAR;
		if (lower.startsWith("timestamp") || lower.startsWith("datetime"))
			return Types.TIMESTAMP;
		if (lower.startsWith("date"))
			return Types.DATE;
		if (lower.startsWith("time"))
			return Types.TIME;
		
		return Types.OTHER;
	}
	
	/**
	 * Searches for a column with the given name
	 * @param columnInfo A list of columns
	 * @param columnName The name of the searched column
	 * @return The column with the given name (not case-sensitive) or null if there isn't one
	 */
	public static ColumnInfo findColumnWithName(Collection<? extends ColumnInfo> columnInfo, 
			String columnName)
	{
		for (ColumnInfo column : columnInfo)
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
	public static ColumnInfo findColumnForAttributeName(DatabaseTable table, 
			String attributeName) throws NoColumnForAttributeException
	{
		ColumnInfo column = findColumnWithName(table.getColumnInfo(), 
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
	 */
	public static class ColumnInfo
	{
		// ATTRIBUTES	----------------------
		
		private String name;
		private boolean autoIncrement, primary, nullAllowed;
		private int type;
		private Object defaultValue;
		
		
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
		public ColumnInfo(String name, int type, boolean nullAllowed, boolean primary, 
				boolean autoIncrement, Object defaultValue)
		{
			this.name = name;
			this.primary = primary;
			this.autoIncrement = autoIncrement;
			this.type = type;
			this.nullAllowed = nullAllowed;
			this.defaultValue = defaultValue;
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
				s.append("default = " + getDefaultValue());
			
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
		 * @see Types
		 */
		public int getType()
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
		public Object getDefaultValue()
		{
			return this.defaultValue;
		}
	}
}
