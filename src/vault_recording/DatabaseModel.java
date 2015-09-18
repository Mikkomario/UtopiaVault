package vault_recording;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import vault_database.DatabaseAccessor;
import vault_database.DatabaseTable;

/**
 * DatabaseModels represent a single row in an indexed table
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class DatabaseModel implements DatabaseReadable, DatabaseWritable
{
	// ATTRIBUTES	--------------------
	
	private Map<String, Attribute> attributes;
	private DatabaseTable table;
	private boolean allowUpdateRewrite;
	
	
	// CONSTRUCTOR	--------------------
	
	/**
	 * Creates a new empty database model
	 * @param table The table the model uses
	 * @param allowUpdateRewrite Does the model allow attribute changes from the database if 
	 * there is already an attribute with the same name
	 * @see DatabaseAccessor#readObjectAttributesFromDatabase(DatabaseReadable, Collection, AttributeNameMapping)
	 */
	public DatabaseModel(DatabaseTable table, boolean allowUpdateRewrite)
	{
		this.attributes = new HashMap<>();
		this.table = table;
		this.allowUpdateRewrite = allowUpdateRewrite;
	}
	
	/**
	 * Creates a new database model with existing attributes
	 * @param table The table the model uses
	 * @param allowUpdateRewrite Does the model allow attribute changes from the database if 
	 * there is already an attribute with the same name
	 * @param attributes The attributes the model will have. The model will use a different 
	 * collection and just copies values from this one
	 * @see DatabaseAccessor#updateObjectToDatabase(DatabaseWritable, boolean)
	 */
	public DatabaseModel(DatabaseTable table, boolean allowUpdateRewrite, 
			Collection<? extends Attribute> attributes)
	{
		this.attributes = new HashMap<>();
		addAttributes(attributes, true);
		this.table = table;
		this.allowUpdateRewrite = allowUpdateRewrite;
	}
	
	
	// IMPLEMENTED METHODS	------------

	@Override
	public DatabaseTable getTable()
	{
		return this.table;
	}

	@Override
	public void newIndexGenerated(int newIndex)
	{
		getIndexAttribute().setValue(newIndex);
	}
	
	@Override
	public Collection<Attribute> getAttributes()
	{
		return this.attributes.values();
	}

	@Override
	public void updateAttributes(Collection<Attribute> readAttributes)
	{
		addAttributes(readAttributes, allowsUpdateRewrite());
	}
	
	
	// ACCESSORS	---------------------------
	
	/**
	 * @return Should the object overwrite it's existing attributes with those read from a 
	 * database
	 */
	public boolean allowsUpdateRewrite()
	{
		return this.allowUpdateRewrite;
	}
	
	
	// OTHER METHODS	-----------------------
	
	/**
	 * Returns one of the model's attributes. Not case-sensitive.
	 * @param attributeName The name of the attribute
	 * @return Model's attribute with the given name, null if no such attribute exists
	 */
	public Attribute getAttribute(String attributeName)
	{
		return DatabaseWritable.getAttributeByName(this, attributeName);
	}
	
	/**
	 * Returns the object's attribute that is associated with the provided column name
	 * @param columnName The name of the column represented by the attribute
	 * @return Model's attribute representing the given column, null if no such attribute 
	 * exists
	 */
	public Attribute getColumnAttribute(String columnName)
	{
		return DatabaseWritable.getAttributeByColumnName(this, columnName);
	}
	
	/**
	 * @return The attribute that contains the model's index
	 */
	public Attribute getIndexAttribute()
	{
		return DatabaseWritable.getIndexAttribute(this);
	}
	
	/**
	 * Checks if the model has an attribute with the given name (not case-sensitive)
	 * @param attributeName The name of the attribute
	 * @return Does the model currently have an attribute with the given name
	 */
	public boolean hasAttributeWithName(String attributeName)
	{
		return this.attributes.containsKey(attributeName);
	}
	
	/**
	 * Adds a single attribute to the model
	 * @param attribute The attribute that will be added
	 * @param replaceIfExists If there already exists an attribute with the same name, should 
	 * it be overwritten by this one
	 */
	public void addAttribute(Attribute attribute, boolean replaceIfExists)
	{
		if (replaceIfExists || this.attributes.containsKey(attribute.getName()))
			this.attributes.put(attribute.getName(), attribute);
	}
	
	/**
	 * Adds multiple attributes to the model
	 * @param attributes The attributes that will be added
	 * @param replaceIfExists If there already exists attributes with same names, should they 
	 * be overwritten
	 */
	public void addAttributes(Collection<? extends Attribute> attributes, boolean replaceIfExists)
	{
		for (Attribute attribute : attributes)
		{
			addAttribute(attribute, replaceIfExists);
		}
	}
}
