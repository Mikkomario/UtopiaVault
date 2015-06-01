package vault_recording;

import vault_database.DatabaseTable;

/**
 * These objects can be read from the database
 * @author Mikko Hilpinen
 * @since 30.5.2015
 */
public interface DatabaseReadable
{
	/**
	 * @return The table that contains the object's data
	 */
	public DatabaseTable getTable();
	
	/**
	 * Changes the object based on the value read from the database
	 * @param columnName The name of the column that contains the value
	 * @param readValue The value read from the database
	 */
	public void setValue(String columnName, String readValue);
}
