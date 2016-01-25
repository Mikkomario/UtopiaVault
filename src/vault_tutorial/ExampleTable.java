package vault_tutorial;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import vault_database_old.AttributeNameMapping;
import vault_database_old.DatabaseTable;
import vault_database_old.DatabaseUnavailableException;
import vault_generics.AfterLastUnderLineRule;

/**
 * This is an example implementation of the {@link DatabaseTable} interface. Notice that the 
 * implementing class is an enumeration.
 * @author Mikko Hilpinen
 * @since 23.11.2015
 * @deprecated See {@link ExampleTables} instead
 */
public enum ExampleTable implements DatabaseTable
{
	/**
	 * The user table. Contains columns "users_id", "users_name" and "users_role". Last of 
	 * which references the ExampleTable.ROLES
	 */
	USERS,
	/**
	 * The role table. Contains columns "role_id" and "role_name".
	 */
	ROLES;
	
	
	// ATTRIBUTES	----------------------
	
	/**
	 * The column information is stored into a static map after it has been initialised. 
	 * One may initialise the information here or when it is requested. This example uses 
	 * the latter practice.
	 * @see #getColumnInfo()
	 */
	private static Map<DatabaseTable, List<Column>> columnInfo = null;
	
	/**
	 * The attribute name mappings for each column are also generated when first requested. 
	 * The mappings are stored in a static map so that they can be easily reused.
	 * @see #getAttributeNameMapping()
	 */
	private static Map<DatabaseTable, AttributeNameMapping> mappings = null;
	
	
	// IMPLEMENTED METHODS	--------------

	@Override
	public boolean usesAutoIncrementIndexing()
	{
		// This method can be implemented by using the other instance methods
		return getPrimaryColumn().usesAutoIncrementIndexing();
	}

	@Override
	public String getDatabaseName()
	{
		// One can use multiple databases, in which case a switch-case structure should be 
		// used. Oftentimes this can be hard-coded, however
		return "test_db";
	}

	@Override
	public String getTableName()
	{
		// A useful practice is to use table names as enumeration fields. One can also 
		// use a switch case here, but it would require updating
		return toString().toLowerCase();
	}

	@Override
	public List<String> getColumnNames()
	{
		// This method can be implemented with a static method introduced in DatabaseTable
		return DatabaseTable.getColumnNamesFromColumnInfo(getColumnInfo());
	}

	@Override
	public Column getPrimaryColumn()
	{
		// This method can be implemented with a static method introduced in DatabaseTable
		return DatabaseTable.findPrimaryColumnInfo(getColumnInfo());
	}

	@Override
	public List<Column> getColumnInfo()
	{
		// The columnInfo is initialised when it is first requested
		if (columnInfo == null)
			columnInfo = new HashMap<>();
		
		// Likewise, each table's information is read only when it is first requested, 
		// after which it is stored
		List<Column> columns = columnInfo.get(this);
		if (columns == null)
		{
			try
			{
				// This method reads the column names, data types and default values from 
				// the database.
				columns = DatabaseTable.readColumnInfoFromDatabase(this);
				columnInfo.put(this, columns);
			}
			catch (DatabaseUnavailableException | SQLException e)
			{
				// The exception should be handled somehow. Oftentimes the program can't be 
				// used if this occurs, so throwing a RuntimeException would be the 
				// appropriate practice
				System.err.println("Failed to read column information");
				e.printStackTrace();
			}
		}
		
		return columns;
	}

	@Override
	public AttributeNameMapping getAttributeNameMapping()
	{
		// The attribute name mappings are initialised when they are first requested
		if (mappings == null)
			mappings = new HashMap<>();
		
		// The mappings for each table are initialised separately at the time they are 
		// first used
		AttributeNameMapping mapping = mappings.get(this);
		if (mapping == null)
		{
			mapping = new AttributeNameMapping();
			// One may use different and / or multiple name mapping rules here
			// This one only takes the part after the last underscore (users_name becomes 
			// name, for example)
			mapping.addRule(AfterLastUnderLineRule.getInstance());
			// This is a nifty method that makes reliable attribute name to column name 
			// mapping possible
			mapping.addMappingForEachColumnWherePossible(getColumnInfo());
			// The mapping should be saved so that it can be reused
			mappings.put(this, mapping);
		}
		
		return mapping;
	}
}
