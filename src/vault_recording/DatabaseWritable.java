package vault_recording;

import java.util.Collection;

import vault_database_old.Attribute;
import vault_database_old.DatabaseTable.Column;
import vault_generics.TableModel;

/**
 * These objects can be written into database.
 * @author Mikko Hilpinen
 * @since 30.5.2015
 * @deprecated {@link TableModel} makes this interface unnecessary
 */
public interface DatabaseWritable extends DatabaseStorable
{
	/**
	 * @return The object's attributes
	 */
	public Collection<Attribute> getAttributes();
	
	/**
	 * This method will be called if a new index is generated for the object through 
	 * auto-increment indexing.
	 * @param newIndex The index that was generated
	 */
	public void newIndexGenerated(int newIndex);
	
	/**
	 * Returns one of the object's attributes. Not case-sensitive.
	 * @param object The object whose attributes are requested
	 * @param attributeName The name of the attribute
	 * @return Model's attribute with the given name, null if no such attribute exists
	 */
	public static Attribute getAttributeByName(DatabaseWritable object, String attributeName)
	{
		for (Attribute attribute : object.getAttributes())
		{
			if (attribute.getName().equalsIgnoreCase(attributeName))
				return attribute;
		}
		
		return null;
	}
	
	/**
	 * Returns one of the object's attributes. Not case-sensitive.
	 * @param object The object whose attributes are requested
	 * @param columnName The name of the column represented by the attribute
	 * @return Model's attribute with the given name, null if no such attribute exists
	 */
	public static Attribute getAttributeByColumnName(DatabaseWritable object, 
			String columnName)
	{
		for (Attribute attribute : object.getAttributes())
		{
			if (attribute.getDescription().getColumnName().equalsIgnoreCase(columnName))
				return attribute;
		}
		
		return null;
	}
	
	/**
	 * Returns the object's index attribute
	 * @param object The object whose attribute is retrieved
	 * @return The attribute that contains the object's index
	 */
	public static Attribute getIndexAttribute(DatabaseWritable object)
	{
		Column primaryColumn = object.getTable().getPrimaryColumn();
		if (primaryColumn == null)
			return null;
		else
			return getAttributeByColumnName(object, primaryColumn.getName());
	}
}
