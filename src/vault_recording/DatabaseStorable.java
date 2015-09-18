package vault_recording;

import vault_database.DatabaseTable;

/**
 * This is the common interface for objects that can be stored into a database
 * @author Mikko Hilpinen
 * @since 17.9.2015
 * @see DatabaseWritable
 * @see DatabaseReadable
 */
public interface DatabaseStorable
{
	/**
	 * @return The table that contains the object's data
	 */
	public DatabaseTable getTable();
}
