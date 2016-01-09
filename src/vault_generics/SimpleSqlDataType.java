package vault_generics;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

import flow_generics.DataType;
import flow_generics.DataTypes;
import flow_generics.Value;

/**
 * These are the data types used in the sql operations
 * @author Mikko Hilpinen
 * @since 9.1.2015
 */
public enum SimpleSqlDataType implements SqlDataType
{
	// TODO: Add initialisation and a parser
	
	/**
	 * Varchar is the sql version of a string, and those two are interchangeable
	 */
	VARCHAR(Types.VARCHAR),
	/**
	 * Sql uses java.sql.Date instead of the LocalDate used by the basic data types
	 */
	DATE(Types.DATE),
	/**
	 * Sql uses it's own java.sql.Time instead of the LocalTime used by the basic data types
	 */
	TIME(Types.TIME),
	/**
	 * Sql uses java.sql.Timestamp instead of the LocalDateTime used by the basic data types
	 */
	TIMESTAMP(Types.TIMESTAMP),
	/**
	 * Basically the same as it's basic data type counterpart
	 */
	BOOLEAN(Types.BOOLEAN),
	/**
	 * The same integer we have come to know and love from the basic data types
	 */
	INT(Types.INTEGER),
	/**
	 * The sql integer used for very large numbers. May be considered the same as the long type 
	 * of the basic data types
	 */
	BIGINT(Types.BIGINT),
	/**
	 * A double type similar to that of the basic data types
	 */
	DOUBLE(Types.DOUBLE);
	
	
	// ATTRIBUTES	-------------------
	
	private final int sqlType;
	
	
	// CONSTRUCTOR	-------------------
	
	private SimpleSqlDataType(int type)
	{
		this.sqlType = type;
	}
	
	
	// IMPLEMENTED METHODS	-----------

	@Override
	public String getName()
	{
		return toString();
	}

	@Override
	public int getSqlType()
	{
		return this.sqlType;
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Parses a value into sql date format and returns the object value
	 * @param value A value
	 * @return The value's object value casted to date
	 */
	public static Date valueToDate(Value value)
	{
		return (Date) value.parseTo(DATE);
	}
	
	/**
	 * Parses a value into sql timestamp format and returns the object value
	 * @param value A value
	 * @return The value's object value casted to timestamp
	 */
	public static Timestamp valueToTimeStamp(Value value)
	{
		return (Timestamp) value.parseTo(TIMESTAMP);
	}
	
	/**
	 * Parses an sql type from a string
	 * @param s A string that represents a data type. For example "varchar(32)", "string" or 
	 * "tinyint"
	 * @return The data type represented by the string
	 * @see Types
	 */
	public static SqlDataType parseSqlType(String s)
	{
		String lower = s.toLowerCase();
		
		switch (lower)
		{
			case "bigint":	return BIGINT;
			case "string":
			case "char":	return VARCHAR;
			case "long":	return BIGINT;
			case "integer": return INT;
			case "boolean": return BOOLEAN;
			case "float":
			case "decimal":
			case "double":	return DOUBLE;
			case "timestamp":
			case "datetime": return TIMESTAMP;
			case "date": 	return DATE;
			case "time":	return TIME;
		}
		
		String modified;
		if (s.startsWith("tiny"))
			modified = s.substring("tiny".length());
		else if (s.startsWith("big"))
			modified = s.substring("big".length());
		else if (s.startsWith("medium"))
			modified = s.substring("medium".length());
		else if (s.startsWith("small"))
			modified = s.substring("small".length());
		else if (s.startsWith("long"))
			modified = s.substring("long".length());
		else
			modified = s;
		
		switch (modified)
		{
			case "text": 
			case "blob":	return VARCHAR;
		}
		
		if (modified.startsWith("varchar"))
			return VARCHAR;
		else if (modified.startsWith("int"))
			return INT;
		
		DataType anyType = DataTypes.parseType(s);
		if (anyType instanceof SqlDataType)
			return (SqlDataType) anyType;
		else
			return null;
	}
}
