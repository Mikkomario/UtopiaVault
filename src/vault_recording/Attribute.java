package vault_recording;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_database.DatabaseTable;
import vault_database.DatabaseTable.ColumnInfo;

/**
 * Attributes represent key value pairs that can also be recorded into database
 * @author Mikko Hilpinen
 * @since 17.9.2015
 */
public class Attribute
{
	// ATTRIBUTES	------------------
	
	private AttributeDescription description;
	private Object value;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new attribute
	 * @param name The name of the attribute
	 * @param columnName The name of the column representing the attribute
	 * @param type The type of the attribute's value
	 * @param value The value stored in the attribute (must be of correct type)
	 * @see Types
	 */
	public Attribute(String name, String columnName, int type, Object value)
	{
		this.description = new AttributeDescription(columnName, name, type);
		this.value = value;
	}
	
	/**
	 * Creates a new attribute
	 * @param columnInfo The column associated with this attribute
	 * @param name The name of the attribute
	 * @param value The attribute's value
	 */
	public Attribute(ColumnInfo columnInfo, String name, Object value)
	{
		this.description = new AttributeDescription(columnInfo, name);
		this.value = value;
	}
	
	/**
	 * Creates a new attribute with the same name as the column's name
	 * @param columnInfo The column associated with this attribute
	 * @param value The attribute's value
	 */
	public Attribute(ColumnInfo columnInfo, Object value)
	{
		this.description = new AttributeDescription(columnInfo);
		this.value = value;
	}
	
	/**
	 * Creates a new attribute
	 * @param description The attribute's description
	 * @param value The attribute's value
	 */
	public Attribute(AttributeDescription description, Object value)
	{
		this.description = description;
		this.value = value;
	}
	
	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toString()
	{
		String start = getName() + " (" + getDescription().getColumnName() + ")  = ";
		try
		{
			if (isNull())
				return start + " null";
			return start + getStringValue();
		}
		catch (InvalidDataTypeException e)
		{
			return start + getValue().toString();
		}
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
	 * @return The uncasted attribute value
	 */
	public Object getValue()
	{
		return this.value;
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * @return Is the attribute's current value set to null
	 */
	public boolean isNull()
	{
		return this.value == null;
	}
	
	/**
	 * Changes the attribute value to null
	 */
	public void setToNull()
	{
		this.value = null;
	}
	
	/**
	 * Changes the attribute's value
	 * @param string The new value
	 * @throws InvalidDataTypeException If the string can't be parsed into a suitable  
	 * type
	 */
	public void setValue(String string) throws InvalidDataTypeException
	{
		if (getDescription().isOfStringType())
			this.value = string;
		else if (getDescription().isOfIntegerType())
			this.value = valueAsInt(string);
		else if (getDescription().isOfNumericType())
			this.value = valueAsDouble(string);
		else if (getDescription().isOfType(Types.DATE))
			this.value = valueAsDate(string);
		else if (getDescription().isOfType(Types.TIMESTAMP))
			this.value = valueAsDateTime(string);
		throw getDataTypeException(Types.VARCHAR);
	}
	
	/**
	 * Changes the attribute's value
	 * @param numeric The new value
	 * @throws InvalidDataTypeException If the value can't be parsed into a suitable type
	 */
	public void setValue(double numeric) throws InvalidDataTypeException
	{
		if (getDescription().isOfIntegerType())
			this.value = (int) numeric;
		else if (getDescription().isOfNumericType())
			this.value = numeric;
		else if (getDescription().isOfStringType())
			this.value = numeric + "";
		throw getDataTypeException(Types.INTEGER);
	}
	
	/**
	 * Changes the attribute's value
	 * @param date The new value
	 * @throws InvalidDataTypeException If the attribute isn't of date type
	 */
	public void setValue(LocalDate date) throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.DATE))
			this.value = Date.valueOf(date);
		else if (getDescription().isOfStringType())
			this.value = date.toString();
		throw getDataTypeException(Types.DATE);
	}
	
	/**
	 * Changes the attribute's value
	 * @param date The new value
	 * @throws InvalidDataTypeException If the attribute isn't of date or timestamp type
	 */
	public void setValue(LocalDateTime date) throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.TIMESTAMP))
			this.value = Timestamp.valueOf(date);
		else if (getDescription().isOfType(Types.DATE))
			setValue(date.toLocalDate());
		else if (getDescription().isOfStringType())
			setValue(date.toString());
		throw getDataTypeException(Types.TIMESTAMP);
	}
	
	/**
	 * Changes the attribute's value
	 * @param bool The new value
	 * @throws InvalidDataTypeException If the value can't be parsed into correct type
	 */
	public void setValue(boolean bool) throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.BOOLEAN))
			this.value = bool;
		else if (getDescription().isOfNumericType())
			this.value = (bool ? 1 : 0);
		else if (getDescription().isOfStringType())
			this.value = (bool ? "true" : "false");
		throw getDataTypeException(Types.BOOLEAN);
	}
	
	/**
	 * Changes the attribute's value. Can't check data types. Invalid type may cause problems 
	 * later.
	 * @param object The attribute's new value.
	 */
	public void setValue(Object object)
	{
		this.value = object;
	}
	
	/**
	 * @return The attribute value as a string
	 * @throws InvalidDataTypeException If the attribute value isn't of string type
	 */
	public String getStringValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfStringType())
			return getValue().toString();
		if (getDescription().isOfType(Types.DATE))
			return getDateValue().toString();
		if (getDescription().isOfType(Types.TIMESTAMP))
			return getDateTimeValue().toString();
		if (getDescription().isOfIntegerType())
			return getIntValue() + "";
		if (getDescription().isOfNumericType())
			return getDoubleValue() + "";
		if (getDescription().isOfType(Types.BOOLEAN))
			return (getBooleanValue() ? "true" : "false");
		throw getDataTypeException(Types.VARCHAR);
	}
	
	/**
	 * @return The attribute value as an integer
	 * @throws InvalidDataTypeException If the attribute value isn't of integer type. The value 
	 * may also be of string type but must represent an integer.
	 */
	public int getIntValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfNumericType())
			return (int) getValue();
		if (getDescription().isOfStringType())
			return valueAsInt(getStringValue());
		throw getDataTypeException(Types.INTEGER);
	}
	
	/**
	 * @return The attribute value as a date
	 * @throws InvalidDataTypeException If the attribute isn't of date type
	 */
	public LocalDate getDateValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.DATE))
			return ((Date) getValue()).toLocalDate();
		if (getDescription().isOfStringType())
			return valueAsDate(getStringValue());
		throw getDataTypeException(Types.DATE);
	}
	
	/**
	 * @return The attribute value as a dateTime
	 * @throws InvalidDataTypeException If the attribute isn't of timestamp type
	 */
	public LocalDateTime getDateTimeValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.TIMESTAMP))
			return ((Timestamp) getValue()).toLocalDateTime();
		if (getDescription().isOfStringType())
			return valueAsDateTime(getStringValue());
		throw getDataTypeException(Types.TIMESTAMP);
	}
	
	/**
	 * @return The attribute value as a boolean
	 * @throws InvalidDataTypeException If the attribute isn't of boolean or tinyint type
	 */
	public boolean getBooleanValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfType(Types.BOOLEAN))
			return (boolean) getValue();
		if (getDescription().isOfType(Types.TINYINT))
			return getIntValue() == 1;
		throw getDataTypeException(Types.BOOLEAN);
	}
	
	/**
	 * @return The attribute value as a double
	 * @throws InvalidDataTypeException If the attribute isn't of integer or double type
	 */
	public double getDoubleValue() throws InvalidDataTypeException
	{
		if (getDescription().isOfNumericType())
			return (double) getValue();
		if (getDescription().isOfStringType())
			return valueAsDouble(getStringValue());
		throw getDataTypeException(Types.DOUBLE);
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
	 * Finds a column from a list of columns that this attribute represents
	 * @param columns A set of columns
	 * @return A column from the set that is represented by this attribute. Null if the 
	 * attribute doesn't represent any of the columns
	 */
	public ColumnInfo findMatchingColumnFrom(Collection<? extends ColumnInfo> columns)
	{
		for (ColumnInfo column : columns)
		{
			if (column.getName().equalsIgnoreCase(getDescription().getColumnName()))
				return column;
		}
		
		return null;
	}
	
	/**
	 * Filters all the attributes that can be stored into the given table
	 * @param attributes The attributes the tableAttributes are picked from
	 * @param table The table the for which the attributes are used for
	 * @return List that contains only attributes that can be updated into the provided table
	 */
	public static List<Attribute> getTableAttributesFrom(Collection<Attribute> attributes, 
			DatabaseTable table)
	{
		List<Attribute> tableAttributes = new ArrayList<>();
		for (Attribute attribute : attributes)
		{
			for (ColumnInfo columnInfo : table.getColumnInfo())
			{
				if (columnInfo.getName().equalsIgnoreCase(attribute.getName()))
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
	public static List<AttributeDescription> getDescriptionsFrom(Collection<Attribute> attributes)
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
	 */
	public static List<AttributeDescription> getDescriptionsFrom(List<ColumnInfo> columnInfo, 
			AttributeNameMapping nameMapping)
	{
		List<AttributeDescription> descriptions = new ArrayList<>();
		for (ColumnInfo column : columnInfo)
		{
			String attributeName = nameMapping.getAttributeName(column.getName());
			descriptions.add(new AttributeDescription(column, attributeName));
		}
		
		return descriptions;
	}
	
	private InvalidDataTypeException getDataTypeException(int usedDataType)
	{
		return new InvalidDataTypeException(usedDataType, getDescription().getType(), this);
	}
	
	private int valueAsInt(String value) throws InvalidDataTypeException
	{
		try
		{
			return Integer.parseInt(value);
		}
		catch (NumberFormatException e)
		{
			throw getDataTypeException(Types.INTEGER);
		}
	}
	
	private double valueAsDouble(String value) throws InvalidDataTypeException
	{
		try
		{
			return Double.parseDouble(value);
		}
		catch (NumberFormatException e)
		{
			throw getDataTypeException(Types.FLOAT);
		}
	}
	
	private LocalDate valueAsDate(String value) throws InvalidDataTypeException
	{
		try
		{
			return LocalDate.parse(value);
		}
		catch (DateTimeParseException e)
		{
			throw getDataTypeException(Types.DATE);
		}
	}
	
	private LocalDateTime valueAsDateTime(String value) throws InvalidDataTypeException
	{
		try
		{
			return LocalDateTime.parse(value);
		}
		catch (DateTimeParseException e)
		{
			throw getDataTypeException(Types.TIMESTAMP);
		}
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
		
		private String columnName, name;
		private int type;
		
		
		// CONSTRUCTOR	---------------
		
		/**
		 * Creates a new description
		 * @param columnName The name of the column associated with this attribute
		 * @param attributeName The attribute's name
		 * @param dataType The data type of the attribute
		 * @see Types
		 */
		public AttributeDescription(String columnName, String attributeName, int dataType)
		{
			this.columnName = columnName;
			this.name = attributeName;
			this.type = dataType;
		}
		
		/**
		 * Creates a new description
		 * @param columnInfo The column that describes the attribute
		 * @param attributeName The name of the attribute
		 */
		public AttributeDescription(ColumnInfo columnInfo, String attributeName)
		{
			this.columnName = columnInfo.getName();
			this.type = columnInfo.getType();
			this.name = attributeName;
		}
		
		/**
		 * Creates a new description. The name of the attribute will be the same as the 
		 * column name
		 * @param columnInfo The column that is associated with the attribute
		 */
		public AttributeDescription(ColumnInfo columnInfo)
		{
			this.columnName = columnInfo.getName();
			this.type = columnInfo.getType();
			this.name = this.columnName;
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
		public int getType()
		{
			return this.type;
		}
		
		/**
		 * @return The name of the column that represents this attribute
		 */
		public String getColumnName()
		{
			return this.columnName;
		}
		
		
		// OTHER METHODS	----------------------
		
		/**
		 * Checks if the attribute's type is the one provided
		 * @param type an sql data type
		 * @return Is this attribute of the given type
		 */
		public boolean isOfType(int type)
		{
			return getType() == type;
		}
		
		/**
		 * @return Is the attribute's type one that represents an integer (int, bigint, 
		 * tinyint, smallint)
		 */
		public boolean isOfIntegerType()
		{
			return isOfType(Types.INTEGER) || isOfType(Types.BIGINT) || isOfType(Types.TINYINT) || 
					isOfType(Types.SMALLINT);
		}
		
		/**
		 * @return Is the attribute's type one that represents a string (varchar, char, 
		 * binary, varbinary)
		 */
		public boolean isOfStringType()
		{
			return isOfType(Types.VARCHAR) || isOfType(Types.CHAR) || isOfType(Types.BINARY) || 
					isOfType(Types.VARBINARY);
		}
		
		/**
		 * @return Is the attribute's type one that represents a (decimal) number (double, 
		 * float, decimal, int, bigint, smallint, tinyint)
		 */
		public boolean isOfNumericType()
		{
			return isOfType(Types.DOUBLE) || isOfType(Types.FLOAT) || isOfType(Types.DECIMAL) || 
					isOfIntegerType();
		}
	}
	
	/**
	 * These exceptions are thrown when one tries to use invalid data types with attributes
	 * @author Mikko Hilpinen
	 * @since 17.9.2015
	 */
	public static class InvalidDataTypeException extends RuntimeException
	{
		// ATTRIBUTES	---------------
		
		private static final long serialVersionUID = -2413041704623953008L;
		private int usedType, correctType;
		private Attribute source;
		
		
		// CONSTRUCTOR	---------------
		
		private InvalidDataTypeException(int usedType, int correctType, Attribute source)
		{
			super("Invalid data type used on attribute " + source.getName());
			
			this.usedType = usedType;
			this.correctType = correctType;
			this.source = source;
		}
		
		
		// ACCESSORS	---------------
		
		/**
		 * @return The data type that was used
		 * @see Types
		 */
		public int getUsedType()
		{
			return this.usedType;
		}
		
		/**
		 * @return The correct data type that should be used
		 * @see Types
		 */
		public int getCorrectType()
		{
			return this.correctType;
		}
		
		/**
		 * @return The attribute that couldn't accept the stored value
		 */
		public Attribute getSourceAttribute()
		{
			return this.source;
		}
	}
}
