package utopia.vault.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.DataType;
import utopia.flow.generics.Value;
import utopia.vault.generics.BasicSqlDataType;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnInitialiser;
import utopia.vault.generics.CurrentTimestamp;
import utopia.vault.generics.Table;
import utopia.vault.generics.TableInitialisationException;
import utopia.vault.generics.VariableNameMapping.NoVariableForColumnException;

/**
 * This column initialiser is able to read the column data from the database using sql queries
 * @author Mikko Hilpinen
 * @since 10.1.2016
 */
public class ReadFromDatabaseColumnInitialiser implements ColumnInitialiser
{
	// ATTRIBUTES	----------------------
	
	private ColumnTypeInterpreter parser = null;
	
	
	// CONSTRUCTOR	----------------------
	
	/**
	 * Creates a new column initialiser
	 * @param typeParser The parser that is able to interpret column data types based on strings. 
	 * Types introduced in {@link BasicSqlDataType} don't need to be handled by this parser. 
	 * Optional, should be used when additional data types are stored in the database.
	 */
	public ReadFromDatabaseColumnInitialiser(ColumnTypeInterpreter typeParser)
	{
		this.parser = typeParser;
	}
	
	
	// IMPLEMENTED METHODS	--------------

	@SuppressWarnings("resource")
	@Override
	public Collection<? extends Column> generateColumns(Table table) throws 
			TableInitialisationException
	{
		Database accessor = new Database(table);
		PreparedStatement statement = null;
		ResultSet result = null;
		List<Column> columns = new ArrayList<>();
		TableInitialisationException exception = null;
		
		try
		{
			statement = accessor.getPreparedStatement("DESCRIBE " + table.getName());
			result = statement.executeQuery();
			// Reads the field names
			while (result.next())
			{
				String name = result.getString("Field");
				boolean primary = "PRI".equalsIgnoreCase(result.getString("Key"));
				boolean autoInc = "auto_increment".equalsIgnoreCase(result.getString("Extra"));
				
				// Parses the data type using the provided parser (if possible)
				String typeString = result.getString("Type");
				DataType type = null;//DataTypes.parseType(typeString); // TODO: Doesn't work
				if (this.parser != null)
					type = this.parser.getColumnType(typeString);
				// If there was no parser or if the parser couldn't read the type, tries the 
				// basic implementation
				if (type == null)
				{
					// Fails if the type can't be parsed
					type = BasicSqlDataType.parseSqlType(typeString);
					if (type == null)
						throw new TableInitialisationException(typeString + 
								" can't be parsed to a data type");
				}
				
				boolean nullAllowed = "YES".equalsIgnoreCase(result.getString("Null"));
				Value defaultValue = null;
				String defaultString = result.getString("Default");
				if ("NULL".equalsIgnoreCase(defaultString))
					defaultValue = Value.NullValue(type);
				else if ("CURRENT_TIMESTAMP".equalsIgnoreCase(defaultString))
					defaultValue = new CurrentTimestamp();
				else
				{
					Value stringValue = Value.String(result.getString("Default"));
					defaultValue = stringValue.castTo(type);
				}
				
				columns.add(new Column(table, name, type, nullAllowed, primary, autoInc, 
						defaultValue));
			}
		}
		catch (DatabaseUnavailableException | SQLException | NoVariableForColumnException e)
		{
			exception = new TableInitialisationException(
					"Failed to read columns for table " + table.getName(), e);
		}
		finally
		{
			// Closes the connections
			Database.closeResults(result);
			Database.closeStatement(statement);
			accessor.closeConnection();
		}
		
		if (exception == null)
			return columns;
		else
			throw exception;
	}
	
	
	// INTERFACES	----------------
	
	/**
	 * These objects are used for interpreting column data types based on database strings
	 * @author Mikko Hilpinen
	 * @since 24.7.2016
	 */
	public static interface ColumnTypeInterpreter
	{
		/**
		 * Finds the data type represented by the provided string
		 * @param typeString The string representing a data type. Example input: 'int(11)', 
		 * 'TINYTEXT', 'float'
		 * @return The data type represented by the string. Null if the string couldn't be 
		 * interpreted
		 */
		public DataType getColumnType(String typeString);
	}
}
