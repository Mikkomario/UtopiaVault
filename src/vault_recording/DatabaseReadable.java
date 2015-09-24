package vault_recording;

import java.util.Collection;

import vault_database.Attribute;

/**
 * These objects can be read from the database
 * @author Mikko Hilpinen
 * @since 30.5.2015
 */
public interface DatabaseReadable extends DatabaseStorable
{
	/**
	 * Updates the object's attributes based on database query
	 * @param readAttributes The attributes read from the database
	 */
	public void updateAttributes(Collection<Attribute> readAttributes);
}
