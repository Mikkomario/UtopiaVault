package vault_database;

import vault_recording.DatabaseWritable;

/**
 * These exceptions are thrown when required index attribute is missing. For example, reading 
 * and updating an object require an index attribute.
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class IndexAttributeRequiredException extends Exception
{
	// ATTRIBUTES	------------------
	
	private static final long serialVersionUID = -6601492011966441821L;
	private DatabaseWritable source;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new exception with custom message
	 * @param source The object that doesn't have an index attribute
	 * @param message The message sent along with the exception
	 */
	public IndexAttributeRequiredException(DatabaseWritable source, String message)
	{
		super(message);
		this.source = source;
	}

	/**
	 * Creates a new exception with generated message
	 * @param source The object that doesn't have an index attribute
	 */
	public IndexAttributeRequiredException(DatabaseWritable source)
	{
		super(source.getTable().getPrimaryColumn() != null ? 
				"Attribute for column " + source.getTable().getPrimaryColumn().getName() + 
				" was not provided" : source.getTable().getTableName() + 
				" doesn't have a primary index column");
	}
	
	
	// ACCESSORS	------------------
	
	/**
	 * @return The object that was missing an index attribute
	 */
	public DatabaseWritable getSourceObject()
	{
		return this.source;
	}
}
