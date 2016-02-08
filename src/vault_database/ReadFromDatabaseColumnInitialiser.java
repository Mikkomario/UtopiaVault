package vault_database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.Value;
import vault_generics.Column;
import vault_generics.ColumnInitialiser;
import vault_generics.Table;
import vault_generics.BasicSqlDataType;
import vault_generics.SqlDataType;
import vault_generics.VariableNameMapping.NoVariableForColumnException;

/**
 * This column initialiser is able to read the column data from the database using sql queries
 * @author Mikko Hilpinen
 * @since 10.1.2016
 */
public class ReadFromDatabaseColumnInitialiser implements ColumnInitialiser
{
	// ATTRIBUTES	---------------------
	
	private DataTypeParser parser;
	
	
	// CONSTRUCTOR	---------------------
	
	/**
	 * Creates a new column initialiser
	 * @param typeParser The parser that interprets the data types for the initialiser. 
	 * If this is left to null, the initialiser will use the simple sql data types introduced 
	 * in this project
	 * @see BasicSqlDataType#parseSqlType(String)
	 */
	public ReadFromDatabaseColumnInitialiser(DataTypeParser typeParser)
	{
		this.parser = typeParser;
	}
	
	
	// IMPLEMENTED METHODS	--------------

	@SuppressWarnings("resource")
	@Override
	public Collection<? extends Column> generateColumns(Table table) throws 
			DatabaseTableInitialisationException
	{
		Database accessor = new Database(table);
		PreparedStatement statement = null;
		ResultSet result = null;
		List<Column> columns = new ArrayList<>();
		DatabaseTableInitialisationException exception = null;
		
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
				SqlDataType type;
				if (this.parser == null)
					type = BasicSqlDataType.parseSqlType(typeString);
				else
					type = this.parser.parseType(typeString);
				
				if (type == null)
					throw new DatabaseTableInitialisationException(typeString + 
							" can't be parsed to a data type");
				
				boolean nullAllowed = "YES".equalsIgnoreCase(result.getString("Null"));
				Value defaultValue = null;
				if ("NULL".equalsIgnoreCase(result.getString("Default")))
					defaultValue = Value.NullValue(type);
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
			exception = new DatabaseTableInitialisationException(
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
	
	
	// INTERFACES	-------------------
	
	/**
	 * These parser classes are used for interpreting the data type from the sql response. 
	 * When new sql data types are introduced in lower projects, a parser should be created 
	 * as well
	 * @author Mikko Hilpinen
	 * @since 10.1.2016
	 */
	public static interface DataTypeParser
	{
		/**
		 * Parses an sql data type from a string
		 * @param s a string that should represent a data type
		 * @return a data type represented by the string. Null if the string doesn't represent 
		 * a data type
		 */
		public SqlDataType parseType(String s);
	}
}
