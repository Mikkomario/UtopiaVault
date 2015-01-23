package vault_database;

/**
 * This class is an interface for all classes who need to represent tables in a database. 
 * DatabaseTables are used in multiTableHandling, for example. The subclasses should be 
 * enumerations rather than normal classes.
 * 
 * @author Mikko Hilpinen
 * @since 25.7.2014
 */
public interface DatabaseTable
{
	// ABSTRACT METHODS	----------------------------------------------------------
	
	/**
	 * @return Is indexing used in this table
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
}
