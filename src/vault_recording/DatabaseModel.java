package vault_recording;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import vault_database.Attribute;
import vault_database.Attribute.AttributeDescription;
import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.DatabaseAccessor;
import vault_database.DatabaseTable;
import vault_database.DatabaseTable.Column;

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
	 * @see DatabaseAccessor#readObjectAttributesFromDatabase(DatabaseReadable, vault_database.DatabaseValue)
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
	
	/**
	 * Creates a new model by copying another one. This method copies the attributes, table, 
	 * and updateRewrite allowing from the other model. Both will use the same name mapping.
	 * @param other The model that is being copied.
	 */
	public DatabaseModel(DatabaseModel other)
	{
		this.attributes = new HashMap<>();
		this.table = other.getTable();
		this.allowUpdateRewrite = other.allowsUpdateRewrite();
		
		// Copies the attributes
		for (Attribute attribute : other.getAttributes())
		{
			addAttribute(new Attribute(attribute), true);
		}
	}
	
	
	// IMPLEMENTED METHODS	------------

	@Override
	public DatabaseTable getTable()
	{
		return this.table;
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
	
	@Override
	public void newIndexGenerated(int newIndex)
	{
		try
		{
			addAttribute(new Attribute(getTable().getPrimaryColumn(), 
					getTable().getAttributeNameMapping(), newIndex), true);
		}
		catch (NoAttributeForColumnException e)
		{
			System.err.println("Couldn't generate a new index attribute since " + 
					e.getColumnName() + "in table " + getTable().getTableName() + 
					" can't be mapped to an attribute name");
			e.printStackTrace();
		}
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
	 * @return The descriptions for the model's existing attributes
	 */
	public List<AttributeDescription> getAttributeDescriptions()
	{
		return Attribute.getDescriptionsFrom(getAttributes());
	}
	
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
	 * Returns one of the model's attributes. Not case-sensitive. If there is no such 
	 * attribute in the model, one may be generated (with default initial value, null if 
	 * default value not present)
	 * @param attributeName The name of the attribute
	 * @param generateIfNotExists If there doesn't exist an attribute with the given name, 
	 * should one be generated (if possible)
	 * @return The requested attribute, possibly generated, null if 
	 * generateIfNotExists was false and there was no attribute with the given name
	 * @throws NoAssociatedColumnExistsException If the generated attribute couldn't 
	 * be mapped to a column in the model's table
	 */
	public Attribute getAttribute(String attributeName, boolean generateIfNotExists) throws 
			NoAssociatedColumnExistsException
	{
		Attribute attribute = getAttribute(attributeName);
		if (attribute == null && generateIfNotExists)
		{
			setAttributeValue(attributeName, null, true);
			return getAttribute(attributeName);
		}
		else
			return attribute;
	}
	
	/**
	 * Changes an attribute's value. If there is no attribute for the given attribute name, 
	 * one may be generated.
	 * @param attributeName The name of the attribute
	 * @param newValue The value the attribute will have
	 * @param generateIfNotExists If the attribute doesn't exist, should one be generated
	 * @throws NoAssociatedColumnExistsException if trying to generate an attribute that 
	 * doesn't represent a column in the model's table
	 */
	public void setAttributeValue(String attributeName, Object newValue, 
			boolean generateIfNotExists)
	{
		Attribute attribute = getAttribute(attributeName);
		if (attribute == null)
		{
			try
			{
				if (generateIfNotExists)
				{
					Column column = getColumnForAttributeName(attributeName);
					if (column == null)
						throw new NoAssociatedColumnExistsException(getTable(), attributeName);
					addAttribute(new Attribute(column, getTable().getAttributeNameMapping(), 
							newValue), true);
				}
			}
			catch (NoAttributeForColumnException e)
			{
				throw new NoAssociatedColumnExistsException(getTable(), e);
			}
		}
		else
		{
			if (newValue != null)
				attribute.setValue(newValue);
			else
				attribute.setToNull();
		}
	}
	
	/**
	 * Changes an attribute's value. If an attribute doesn't exist, the value isn't added
	 * @param attributeName The name of the attribute
	 * @param newValue The new value of the attribute (if one exists)
	 */
	public void setAttributeValue(String attributeName, Object newValue)
	{
		Attribute attribute = getAttribute(attributeName);
		if (attribute != null)
			attribute.setValue(newValue);
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
		if (attribute != null && (replaceIfExists || 
				!this.attributes.containsKey(attribute.getName())))
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
	
	/**
	 * Updates each model attribute in the collection with the new value. Doesn't add new 
	 * attributes and only overwrites existing values.
	 * @param attributes The attributes that will be updated to the model.
	 */
	public void updateExistingAttributes(Collection<? extends Attribute> attributes)
	{
		for (Attribute attribute : attributes)
		{
			if (hasAttributeWithName(attribute.getName()))
				addAttribute(attribute, true);
		}
	}
	
	/**
	 * Finds the column that would be represented by the provided attribute name
	 * @param attributeName The name of the attribute that would represent a column
	 * @return A column represented by the provided attribute name or null if no such column 
	 * exists in the model's table
	 * @throws NoAttributeForColumnException If the operation failed because a column name 
	 * couldn't be mapped
	 */
	public Column getColumnForAttributeName(String attributeName) throws NoAttributeForColumnException
	{
		return getTable().getAttributeNameMapping().findColumnForAttribute(
				getTable().getColumnInfo(), attributeName);
	}
	
	/**
	 * Goes through the columns in the database table and creates null value attributes for 
	 * each column
	 * @param keepExistingValues Should existing column attributes be kept as they are (true) 
	 * or replaced with null (false)
	 * @throws NoAttributeForColumnException If one of the column names in the table couldn't 
	 * be mapped to an attribute name. No changes were made to the model in this case.
	 */
	public void initializeTableAttributesToNull(boolean keepExistingValues) throws NoAttributeForColumnException
	{
		List<Attribute> newAttributes = new ArrayList<>();
		
		for (Column column : getTable().getColumnInfo())
		{
			Attribute attribute = new Attribute(column, getTable().getAttributeNameMapping(), 
					null);
			newAttributes.add(attribute);
		}
		
		// Only overwrites if allowed
		addAttributes(newAttributes, !keepExistingValues);
	}
	
	
	// SUBCLASSES	----------------------------
	
	/**
	 * These exceptions are thrown when an attribute can't be generated based on an attribute 
	 * name
	 * @author Mikko Hilpinen
	 * @since 22.9.2015
	 */
	public static class NoAssociatedColumnExistsException extends RuntimeException
	{
		private static final long serialVersionUID = 39359614563601185L;

		/**
		 * Creates a new exception
		 * @param columnName The name of the column the attribute would have had
		 * @param targetTable The table the attribute couldn't be put to
		 */
		public NoAssociatedColumnExistsException(DatabaseTable targetTable, String columnName)
		{
			super("No associated column '" + columnName + "' in table " + 
					targetTable.getTableName());
		}
		
		private NoAssociatedColumnExistsException(DatabaseTable targetTable, 
				NoAttributeForColumnException source)
		{
			super("Can't use column '" + source.getColumnName() + "' in " + 
					targetTable.getTableName() + " since it can't be mapped to an attribute", 
					source);
		}
	}
	
	/**
	 * These exceptions are thrown when an attribute is used that doesn't exist in the 
	 * model
	 * @author Mikko Hilpinen
	 * @since 22.10.2015
	 */
	/*
	public static class NoSuchAttributeExistsException extends RuntimeException
	{
		private static final long serialVersionUID = -3336141768045425709L;

		/**
		 * Creates a new exception
		 * @param table The table the model is from
		 * @param attributeName The name of the requested attribute
		 */
	/*
		public NoSuchAttributeExistsException(DatabaseTable table, String attributeName)
		{
			super("No attribute with name '" + attributeName + "' in model from table " + 
					table);
		}
	}
	*/
}
