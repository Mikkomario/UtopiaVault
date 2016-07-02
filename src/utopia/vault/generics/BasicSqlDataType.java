package utopia.vault.generics;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

import utopia.flow.generics.BasicDataType;
import utopia.flow.generics.DataType;
import utopia.flow.generics.DataTypeTreeNode;
import utopia.flow.generics.DataTypes;
import utopia.flow.generics.Value;

/**
 * These are the data types used in the sql operations. Remember to initialise the data types 
 * before use
 * @author Mikko Hilpinen
 * @since 9.1.2015
 * @see #initialise()
 */
public enum BasicSqlDataType implements SqlDataType
{
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
	DOUBLE(Types.DOUBLE),
	/**
	 * A float type similar to that of the basic data types
	 */
	FLOAT(Types.FLOAT);
	
	
	// ATTRIBUTES	-------------------
	
	private static boolean initialised = false;
	private final int sqlType;
	
	
	// CONSTRUCTOR	-------------------
	
	private BasicSqlDataType(int type)
	{
		this.sqlType = type;
	}
	
	
	// IMPLEMENTED METHODS	-----------

	@Override
	public String toString()
	{
		return "SQL_" + super.toString();
	}
	
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
	 * Parses a value into sql time format and returns the object value
	 * @param value a value
	 * @return The value's object value casted to time
	 */
	public static java.sql.Time valueToTime(Value value)
	{
		return (java.sql.Time) value.parseTo(TIME);
	}
	
	/**
	 * Wraps a date into a value
	 * @param date sql date
	 * @return The date wrapped to value
	 */
	public static Value Date(Date date)
	{
		return new Value(date, DATE);
	}
	
	/**
	 * Wraps a sql time into a value
	 * @param time sql time
	 * @return Time wrapped to value
	 */
	public static Value Time(java.sql.Time time)
	{
		return new Value(time, TIME);
	}
	
	/**
	 * Wraps a timestamp to value
	 * @param timestamp a sql timestamp
	 * @return The timestamp wrapped to value
	 */
	public static Value Timestamp(Timestamp timestamp)
	{
		return new Value(timestamp, TIMESTAMP);
	}
	
	/**
	 * Initialises the sql data types so that they are recognised by parsing operations. This 
	 * method is automatically called when DatabaseSettings are initialised, but can be called 
	 * separately if necessary
	 */
	public static void initialise()
	{
		if (!initialised)
		{
			// Introduces the data types
			DataTypes types = DataTypes.getInstance();
			// Int, bigint, float and double go under the number, the others go under any
			DataTypeTreeNode number = types.get(BasicDataType.NUMBER);
			types.add(new DataTypeTreeNode(INT, number));
			types.add(new DataTypeTreeNode(BIGINT, number));
			types.add(new DataTypeTreeNode(DOUBLE, number));
			types.add(new DataTypeTreeNode(FLOAT, number));
			
			DataTypeTreeNode object = types.get(BasicDataType.OBJECT);
			types.add(new DataTypeTreeNode(VARCHAR, object));
			types.add(new DataTypeTreeNode(DATE, object));
			types.add(new DataTypeTreeNode(TIME, object));
			types.add(new DataTypeTreeNode(TIMESTAMP, object));
			types.add(new DataTypeTreeNode(BOOLEAN, object));
			
			// Adds the parser
			types.addParser(SqlValueParser.getInstance());
			
			initialised = true;
		}
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
			case "string":
			case "char":	return VARCHAR;
			case "long":	return BIGINT;
			case "integer": return INT;
			case "boolean": return BOOLEAN;
			case "decimal":
			case "double":	return DOUBLE;
			case "float":	return FLOAT;
			case "timestamp":
			case "datetime": return TIMESTAMP;
			case "date": 	return DATE;
			case "time":	return TIME;
		}
		
		if (lower.startsWith("bigint"))
			return BIGINT;
		else if (lower.startsWith("tinyint"))
		{
			// tinyint(1) is considered a boolean, a larger tinyint will be considered an 
			// integer
			if (lower.equals("tinyint(1)"))
				return BOOLEAN;
			else
				return INT;
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
