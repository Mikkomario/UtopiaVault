package vault_recording;

import java.util.Collection;

import vault_database_old.Attribute;
import vault_generics.TableModel;

/**
 * These objects can be read from the database
 * @author Mikko Hilpinen
 * @since 30.5.2015
 * @deprecated {@link TableModel} makes this interface unnecessary
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
