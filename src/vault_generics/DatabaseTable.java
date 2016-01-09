package vault_generics;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import flow_generics.DataType;
import flow_generics.Value;
import vault_database.AttributeNameMapping;
import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.DatabaseAccessor;
import vault_database.DatabaseUnavailableException;

/**
 * A database table represents a table in a database and contains the necessary column 
 * information
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class DatabaseTable
{
	// ATTRIBUTES	-------------
	
	private String databaseName, name;
	private AttributeNameMapping nameMapping;
	
	private List<Column> columns = null;
	private Column primaryColumn = null;
	
	// TODO: Add a mapping generator class
	// TODO: Add model declaration generation
	// TODO: Add a new fuzzy logic name mapper class
	
	
	// CONSTRUCTOR	-------------
	
	/**
	 * Creates a new database table
	 * @param databaseName The name of the database the table uses
	 * @param name The name of the table
	 * @param nameMapping The name mapping the table uses
	 */
	public DatabaseTable(String databaseName, String name, AttributeNameMapping nameMapping)
	{
		this.databaseName = databaseName;
		this.name = name;
		this.nameMapping = nameMapping;
	}
	
	
	// IMPLEMENTED METHODS	-----
	
	@Override
	public String toString()
	{
		return getName();
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((getDatabaseName() == null) ? 0 : getDatabaseName().hashCode());
		result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DatabaseTable))
			return false;
		DatabaseTable other = (DatabaseTable) obj;
		if (getDatabaseName() == null)
		{
			if (other.getDatabaseName() != null)
				return false;
		}
		else if (!getDatabaseName().equals(other.getDatabaseName()))
			return false;
		if (getName() == null)
		{
			if (other.getName() != null)
				return false;
		}
		else if (!getName().equals(other.name))
			return false;
		return true;
	}
	
	
	// ACCESSORS	-------------

	/**
	 * @return The name of the database the table uses
	 */
	public String getDatabaseName()
	{
		return this.databaseName;
	}
	
	/**
	 * @return The name of the table in the database
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @return The attribute name mapping that defines the variable names for the columns
	 */
	public AttributeNameMapping getNameMapping()
	{
		return this.nameMapping;
	}
	
	/**
	 * @return The columns in this table
	 * @throws DatabaseTableInitialisationException If the columns couldn't be initialised, 
	 * for some reason
	 */
	public List<? extends Column> getColumns() throws DatabaseTableInitialisationException
	{
		// Reads the column data from the database when first requested
		if (this.columns == null)
		{
			this.columns = readColumnsFromDatabase();
			// TODO: Also finalise the name mapping
		}
		
		return this.columns;
	}
	
	/**
	 * @return The primary column in this table. Null if there is no primary column
	 */
	public Column getPrimaryColumn()
	{
		if (this.primaryColumn == null)
		{
			for (Column column : getColumns())
			{
				if (column.isPrimary())
				{
					this.primaryColumn = column;
					break;
				}
			}
		}
		
		return this.primaryColumn;
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * @return A list containing the name of each column in the table
	 */
	public List<String> getColumnNames()
	{
		List<String> names = new ArrayList<>();
		for (Column column : getColumns())
		{
			names.add(column.getColumnName());
		}
		
		return names;
	}
	
	/**
	 * @return A list containing the variable names of the columns in the table
	 */
	public List<String> getColumnVariableNames()
	{
		List<String> names = new ArrayList<>();
		for (Column column : getColumns())
		{
			names.add(column.getName());
		}
		
		return names;
	}
	
	/**
	 * Finds a column in this table that has the provided variable name
	 * @param variableName The name of the column variable
	 * @return A column in this table with the provided name. Null if no such column exists.
	 */
	public Column findColumnWithVariableName(String variableName)
	{
		for (Column column : getColumns())
		{
			if (column.getName().equalsIgnoreCase(variableName))
				return column;
		}
		
		return null;
	}
	
	/**
	 * Finds a column in this table that has the provided column name
	 * @param columnName The name of a column in the database
	 * @return A column in this table with the provided column name. Null if no such column exists.
	 */
	public Column findColumnWithColumnName(String columnName)
	{
		for (Column column : getColumns())
		{
			if (column.getColumnName().equalsIgnoreCase(columnName))
				return column;
		}
		
		return null;
	}
	
	private List<Column> readColumnsFromDatabase() throws DatabaseTableInitialisationException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(getDatabaseName());
		PreparedStatement statement = null;
		ResultSet result = null;
		List<Column> columns = new ArrayList<>();
		DatabaseTableInitialisationException exception = null;
		
		try
		{
			statement = accessor.getPreparedStatement("DESCRIBE " + getName());
			result = statement.executeQuery();
			// Reads the field names
			while (result.next())
			{
				String name = result.getString("Field");
				boolean primary = "PRI".equalsIgnoreCase(result.getString("Key"));
				boolean autoInc = "auto_increment".equalsIgnoreCase(result.getString("Extra"));
				DataType type = SimpleSqlDataType.parseSqlType(result.getString("Type"));
				boolean nullAllowed = "YES".equalsIgnoreCase(result.getString("Null"));
				Value defaultValue = null;
				if ("NULL".equalsIgnoreCase(result.getString("Default")))
					defaultValue = Value.NullValue(type);
				else
				{
					Value stringValue = Value.String(result.getString("Default"));
					defaultValue = stringValue.castTo(type);
				}
				
				columns.add(new Column(this, name, type, nullAllowed, primary, autoInc, 
						defaultValue));
			}
		}
		catch (DatabaseUnavailableException | SQLException | NoAttributeForColumnException e)
		{
			exception = new DatabaseTableInitialisationException(
					"Failed to read columns for table " + getName(), e);
		}
		finally
		{
			// Closes the connections
			DatabaseAccessor.closeResults(result);
			DatabaseAccessor.closeStatement(statement);
			accessor.closeConnection();
		}
		
		if (exception == null)
			return columns;
		else
			throw exception;
	}
	
	
	// NESTED CLASSES	----------------------
	
	/**
	 * These exceptions are thrown when database table initialisation (reading column 
	 * information, etc) fails
	 * @author Mikko Hilpinen
	 * @since 9.1.2016
	 */
	public static class DatabaseTableInitialisationException extends RuntimeException
	{
		private static final long serialVersionUID = -4211842208696476343L;

		private DatabaseTableInitialisationException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}
}
