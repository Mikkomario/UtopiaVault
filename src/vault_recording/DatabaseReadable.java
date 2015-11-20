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
	 * Updates the object's attributes, possibly adding new ones. Previous attributes or 
	 * attribute values may be overwritten.
	 * @param attributes The new attributes stored in the model
	 */
	public void addAttributes(Collection<Attribute> attributes);
}
