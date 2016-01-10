package vault_recording;

import vault_database_old.DatabaseTable;
import vault_generics.TableModel;

/**
 * This is the common interface for objects that can be stored into a database
 * @author Mikko Hilpinen
 * @since 17.9.2015
 * @see DatabaseWritable
 * @see DatabaseReadable
 * @deprecated {@link TableModel} makes this interface unnecessary
 */
public interface DatabaseStorable
{
	/**
	 * @return The table that contains the object's data
	 */
	public DatabaseTable getTable();
}
