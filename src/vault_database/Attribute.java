package vault_database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DataType.InvalidDataTypeException;
import vault_database.DatabaseTable.Column;

/**
 * Attributes represent key value pairs that can also be recorded into database
 * @author Mikko Hilpinen
 * @since 17.9.2015
 */
public class Attribute
{
	// ATTRIBUTES	------------------
	
	private AttributeDescription description;
	private DatabaseValue value;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new attribute
	 * @param columnInfo The column associated with this attribute
	 * @param name The name of the attribute
	 * @param value The attribute's value
	 */
	public Attribute(Column columnInfo, String name, Object value)
	{
		this.description = new AttributeDescription(columnInfo, name);
		setValue(value);
	}
	
	/**
	 * Creates a new attribute
	 * @param description The attribute's description
	 * @param value The attribute's value
	 */
	public Attribute(AttributeDescription description, Object value)
	{
		this.description = description;
		setValue(value);
	}
	
	/**
	 * Creates a new attribute
	 * @param column The column associated with this attribute
	 * @param nameMapping The column name to attribute name -mapping
	 * @param value The attribute's value
	 * @throws NoAttributeForColumnException If the column name couldn't be mapped to an 
	 * attribute name
	 */
	public Attribute(Column column, AttributeNameMapping nameMapping, Object value) 
			throws NoAttributeForColumnException
	{
		this.description = new AttributeDescription(column, nameMapping);
		setValue(value);
	}
	
	/**
	 * Creates a new table attribute
	 * @param table The table the attribute is from
	 * @param columnName The name of the column the attribute represents
	 * @param value The value the attribute will have
	 * @throws NoAttributeForColumnException If there is no column with the given name or no 
	 * attribute name for the column name
	 */
	public Attribute(DatabaseTable table, String columnName, Object value) throws 
			NoAttributeForColumnException
	{
		this.description = getColumnDescription(table, columnName);
		setValue(value);
	}
	
	/**
	 * Creates a copy of another attribute
	 * @param other The attribute that is copied
	 */
	public Attribute(Attribute other)
	{
		this.description = other.getDescription();
		this.value = other.getValue();
	}
	
	/**
	 * Creates a new table attribute with the provided value
	 * @param table The table which contains the column the attribute represents
	 * @param attributeName The name of the attribute
	 * @param value The value of the attribute
	 * @return An attribute that represents one of the table's columns
	 * @throws NoColumnForAttributeException If the attribute name can't be mapped to any 
	 * column
	 */
	public static Attribute createTableAttribute(DatabaseTable table, String attributeName, 
			Object value) throws NoColumnForAttributeException
	{
		return new Attribute(getTableAttributeDescription(table, attributeName), value);
	}
	
	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder(getName());
		s.append(" (");
		s.append(getDescription().getColumnName());
		s.append(") = ");
		s.append(getValue().getDescription());
		
		return s.toString();
	}

	
	// ACCESSORS	------------------
	
	/**
	 * @return The description that contains attribute's information (name, columnName, type)
	 */
	public AttributeDescription getDescription()
	{
		return this.description;
	}
	
	/**
	 * @return The name of the attribute
	 */
	public String getName()
	{
		return getDescription().getName();
	}
	
	/**
	 * @return The value of the attribute in general form
	 */
	public DatabaseValue getValue()
	{
		return this.value;
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * @return Is the attribute's current value set to null
	 */
	public boolean isNull()
	{
		return getValue().isNull();
	}
	
	/**
	 * Changes the attribute value to null. If the attribute has a default value and can't be 
	 * set to null, that default value is set instead.
	 */
	public void setToNull()
	{
		// If there is a default value and null not allowed, sets to default
		if (!getDescription().getColumn().nullAllowed())
		{
			DatabaseValue defaultValue = getDescription().getColumn().getDefaultValue();
			if (!defaultValue.isNull())
			{
				setValue(defaultValue);
				return;
			}
		}
		
		this.value = DatabaseValue.nullValue(getDescription().getType());
	}
	
	/**
	 * Changes the attribute's value.
	 * @param object The attribute's new value. Either an object or a database value goes.
	 * @throws InvalidDataTypeException If the provided value can't be parsed to the 
	 * attribute's data type
	 */
	public void setValue(Object object) throws InvalidDataTypeException
	{
		if (object == null)
			setToNull();
		else if (object instanceof DatabaseValue)
			setValue((DatabaseValue) object);
		else
			this.value = DatabaseValue.objectValue(getDescription().getType(), object);
	}
	
	/**
	 * Changes the attribute's value
	 * @param value The attribute's new value in database value format
	 * @throws InvalidDataTypeException If the provided value can't be parsed to the 
	 * attribute's data type
	 */
	public void setValue(DatabaseValue value) throws InvalidDataTypeException
	{
		if (value.isNull())
			setToNull();
		else
			this.value = new DatabaseValue(getDescription().getType(), value);
	}
	
	/**
	 * @return The attribute wrapped into a list
	 */
	public List<Attribute> wrapIntoList()
	{
		List<Attribute> list = new ArrayList<>();
		list.add(this);
		return list;
	}
	
	/**
	 * Checks if two attributes are the same (have same name, type, column and value), 
	 * case-insensitive.
	 * @param other The other attribute
	 * @return Are the two attributes the same.
	 */
	public boolean equals(Attribute other)
	{
		if (other == null)
			return false;
		
		if (!getDescription().equals(other.getDescription()))
			return false;
		
		return hasSameValueAs(other);
	}
	
	/**
	 * Checks if the values in the two attributes are the same. Case-insensitive.
	 * @param other The other attribute
	 * @return Are the values in the attributes the same
	 */
	public boolean hasSameValueAs(Attribute other)
	{
		if (other == null)
			return false;
		
		return getValue().equals(other.getValue());
	}
	
	/**
	 * Filters all the attributes that can be stored into the given table
	 * @param attributes The attributes the tableAttributes are picked from
	 * @param table The table the for which the attributes are used for
	 * @return List that contains only attributes that can be updated into the provided table
	 */
	public static List<Attribute> getTableAttributesFrom(Collection<? extends Attribute> 
			attributes, DatabaseTable table)
	{
		List<Attribute> tableAttributes = new ArrayList<>();
		for (Attribute attribute : attributes)
		{
			for (Column columnInfo : table.getColumnInfo())
			{
				if (columnInfo.getName().equalsIgnoreCase(
						attribute.getDescription().getColumnName()))
				{
					tableAttributes.add(attribute);
					break;
				}
			}
		}
		
		return tableAttributes;
	}
	
	/**
	 * Collects all the attribute descriptions from an attribute collection
	 * @param attributes an attribute collection
	 * @return The descriptions of the attributes in the collection
	 */
	public static List<AttributeDescription> getDescriptionsFrom(Collection<? extends 
			Attribute> attributes)
	{
		List<AttributeDescription> descriptions = new ArrayList<>();
		for (Attribute attribute : attributes)
		{
			descriptions.add(attribute.getDescription());
		}
		
		return descriptions;
	}
	
	/**
	 * Parses attribute descriptions from a table's column information
	 * @param columnInfo The table's column information
	 * @param nameMapping the mapping that associates column names with attribute names
	 * @return An attribute description for each column
	 * @throws NoAttributeForColumnException If a column name couldn't be mapped 
	 * to an attribute name
	 */
	public static List<AttributeDescription> getDescriptionsFrom(Collection<? extends Column> 
			columnInfo, AttributeNameMapping nameMapping) throws NoAttributeForColumnException
	{
		List<AttributeDescription> descriptions = new ArrayList<>();
		for (Column column : columnInfo)
		{
			descriptions.add(new AttributeDescription(column, nameMapping));
		}
		
		return descriptions;
	}
	
	/**
	 * Parses attribute descriptions from a table's column information
	 * @param table The table the column information is read from
	 * @return The attribute descriptions for the table's columns
	 * @throws NoAttributeForColumnException If a column name couldn't be mapped 
	 * to an attribute name
	 */
	public static List<AttributeDescription> getDescriptionsFrom(DatabaseTable table) throws 
			NoAttributeForColumnException
	{
		return getDescriptionsFrom(table.getColumnInfo(), table.getAttributeNameMapping());
	}
	
	/**
	 * Creates a new attribute description based on a database table column
	 * @param table The database table
	 * @param columnName A column name in the table
	 * @return A new attribute description for the column or null if no such column exists 
	 * in the table
	 * @throws NoAttributeForColumnException If the column name couldn't be mapped to an 
	 * attribute name
	 */
	public static AttributeDescription getColumnDescription(DatabaseTable table, 
			String columnName) throws NoAttributeForColumnException
	{
		Column column = DatabaseTable.findColumnWithName(
				table.getColumnInfo(), columnName);
		if (column == null)
			return null;
		return new AttributeDescription(column, table.getAttributeNameMapping());
	}
	
	/**
	 * Creates a new attribute description for a table's attribute
	 * @param table The database table
	 * @param attributeName An attribute that should be associated with a column in the table
	 * @return An attribute description for the table's attribute column or null if no such 
	 * column exists
	 * @throws NoColumnForAttributeException If the attribute name couldn't be retraced back 
	 * to a column name
	 */
	public static AttributeDescription getTableAttributeDescription(DatabaseTable table, 
			String attributeName) throws NoColumnForAttributeException
	{
		Column column = DatabaseTable.findColumnForAttributeName(table, attributeName);
		if (column == null)
			return null;
		return new AttributeDescription(column, attributeName);
	}
	
	
	// SUBCLASSES	-------------------
	
	/**
	 * AttributeDescription describes an attribute. The description contains attribute name, 
	 * type and column name information but not value information.
	 * @author Mikko Hilpinen
	 * @since 18.9.2015
	 */
	public static class AttributeDescription
	{
		// ATTRIBUTES	---------------
		
		private String name;
		private Column column;
		
		
		// CONSTRUCTOR	---------------
		
		/**
		 * Creates a new description
		 * @param columnInfo The column this attribute is associated with
		 * @param attributeName The name of the attribute
		 */
		public AttributeDescription(Column columnInfo, String attributeName)
		{
			this.column = columnInfo;
			this.name = attributeName.toLowerCase();
		}
		
		/**
		 * Creates a new description. The attribute name will be read from the name mapping
		 * @param column The column this attribute is based on
		 * @param nameMapping The column name to attribute name -mappings
		 * @throws NoAttributeForColumnException If the mapping couldn't be used for finding 
		 * the attribute name
		 */
		public AttributeDescription(Column column, AttributeNameMapping nameMapping) 
				throws NoAttributeForColumnException
		{
			this.column = column;
			this.name = nameMapping.getAttributeName(column.getName());
		}
		
		
		// ACCESSORS	--------------------------
		
		/**
		 * @return The name of the attribute
		 */
		public String getName()
		{
			return this.name;
		}
		
		/**
		 * @return The data type of the attribute
		 */
		public DataType getType()
		{
			return getColumn().getType();
		}
		
		/**
		 * @return The name of the column that represents this attribute
		 */
		public String getColumnName()
		{
			return getColumn().getName();
		}
		
		/**
		 * @return The column associated with this attribute
		 */
		public Column getColumn()
		{
			return this.column;
		}
		
		
		// OTHER METHODS	----------------------
		
		/**
		 * @return The attribute wrapped into a list
		 */
		public List<AttributeDescription> wrapIntoList()
		{
			List<AttributeDescription> list = new ArrayList<>();
			list.add(this);
			return list;
		}
		
		/**
		 * Checks if two descirptions are the same
		 * @param other The other description
		 * @return are the two descriptions the same
		 */
		public boolean equals(AttributeDescription other)
		{
			return getColumnName().equalsIgnoreCase(other.getColumnName()) && 
					getName().equalsIgnoreCase(other.getName()) && getType().equals(other.getType());
		}
	}
}
