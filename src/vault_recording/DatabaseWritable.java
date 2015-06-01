package vault_recording;

import vault_database.DatabaseTable;

/**
 * These objects can be written into database.
 * @author Mikko Hilpinen
 * @since 30.5.2015
 */
public interface DatabaseWritable
{
	/**
	 * @return The table this object should be written into
	 */
	public DatabaseTable getTable();
	
	/**
	 * @param columnName The name of the column that needs to be written
	 * @return A value that will be saved into the column
	 */
	public String getColumnValue(String columnName);
	
	/**
	 * This method will be called if a new index is generated for the object through 
	 * auto-increment indexing.
	 * @param newIndex The index that was generated
	 */
	public void newIndexGenerated(int newIndex);
}
