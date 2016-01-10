package vault_database_old;

import java.sql.Types;

/**
 * This represents a data type usable with the databases
 * @author Mikko Hilpinen
 * @since 2.10.2015
 * @deprecated Use vault_generics.SqlDataType instead
 */
public class DataType
{
	// ATTRIBUTES	---------------
	
	/**
	 * The date time type (aka. timestamp)
	 */
	public static DataType DATETIME = new DataType(Types.TIMESTAMP);
	/**
	 * The date type
	 */
	public static DataType DATE = new DataType(Types.DATE);
	/**
	 * The long type (aka. bigint)
	 */
	public static DataType LONG = new DataType(Types.BIGINT);
	/**
	 * The integer type
	 */
	public static DataType INTEGER = new DataType(Types.INTEGER);
	/**
	 * The double type
	 */
	public static DataType DOUBLE = new DataType(Types.DOUBLE);
	/**
	 * The boolean type
	 */
	public static DataType BOOLEAN = new DataType(Types.BOOLEAN);
	/**
	 * The string type (aka. varchar)
	 */
	public static DataType STRING = new DataType(Types.VARCHAR);
	/**
	 * The unknown / other type
	 */
	public static DataType OTHER = new DataType(Types.OTHER);
	
	private int type;
	
	
	// CONSTRUCTOR	---------------
	
	/**
	 * Creates a new database type for the provided sql type
	 * @param sqlType The sql typ
	 * @see Types
	 */
	public DataType(int sqlType)
	{
		this.type = sqlType;
	}
	
	/**
	 * Creates a new database type for the provided sql type string representation
	 * @param typeString The string representation of an sql type
	 */
	public DataType(String typeString)
	{
		String lower = typeString.toLowerCase();
		int type;
		switch (lower)
		{
			case "string": type = Types.VARCHAR; break;
			case "long": type = Types.BIGINT; break;
			default: type = parseSqlType(lower); break;
		}
		
		this.type = type;
	}
	
	
	// IMPLEMENTED METHODS	--------
	
	@Override
	public boolean equals(Object other)
	{
		if (other instanceof DataType)
			return equals((DataType) other);
		
		return super.equals(other);
	}
	
	/*
	 * Value hierarchy:
	 * - Null
	 * 
	 * - DateTime
	 * - Date
	 * 
	 * - Long
	 * - Int
	 * - Double
	 * - Boolean
	 * 
	 * - String
	 */
	
	@Override
	public String toString()
	{
		if (isOfDateTimeType())
			return "dateTime";
		if (isOfDateType())
			return "date";
		if (isOfLongType())
			return "long";
		if (isOfIntegerType())
			return "integer";
		if (isOfNumericType())
			return "double";
		if (isOfBooleanType())
			return "boolean";
		if (isOfStringType())
			return "string";
		if (isOfSqlType(Types.OTHER))
			return "other";
		
		return "?";
	}

	
	// ACCESSORS	---------------
	
	/**
	 * @return The sql type of this database type
	 */
	public int toSqlType()
	{
		return this.type;
	}
	
	
	// OTHER METHODS	-----------
	
	/**
	 * Checks if the two type instances are the same
	 * @param other The other type instance
	 * @return Are the two instances the same
	 */
	public boolean equals(DataType other)
	{
		return this.type == other.type;
	}
	
	/**
	 * Checks if the type represents the one provided
	 * @param sqlType an sql data type
	 * @return Is this attribute of the given type
	 */
	public boolean isOfSqlType(int sqlType)
	{
		return toSqlType() == sqlType;
	}
	
	/**
	 * @return If the type represents a long (bigint)
	 */
	public boolean isOfLongType()
	{
		return isOfSqlType(Types.BIGINT);
	}
	
	/**
	 * @return If the type represents an integer (int, bigint, 
	 * tinyint, smallint)
	 */
	public boolean isOfIntegerType()
	{
		return isOfSqlType(Types.INTEGER) || isOfSqlType(Types.BIGINT) || 
				isOfSqlType(Types.TINYINT) || isOfSqlType(Types.SMALLINT);
	}
	
	/**
	 * @return If the type represents a string (varchar, char, 
	 * binary, varbinary)
	 */
	public boolean isOfStringType()
	{
		return isOfSqlType(Types.VARCHAR) || isOfSqlType(Types.CHAR) || 
				isOfSqlType(Types.BINARY) || isOfSqlType(Types.VARBINARY);
	}
	
	/**
	 * @return If the type represents a (decimal) number (double, 
	 * float, decimal, int, bigint, smallint, tinyint)
	 */
	public boolean isOfNumericType()
	{
		return isOfSqlType(Types.DOUBLE) || isOfSqlType(Types.FLOAT) || 
				isOfSqlType(Types.DECIMAL) || isOfIntegerType();
	}
	
	/**
	 * @return If the type represents a boolean value (boolean, tinyint)
	 */
	public boolean isOfBooleanType()
	{
		return isOfSqlType(Types.BOOLEAN) || isOfSqlType(Types.TINYINT);
	}
	
	/**
	 * @return if the type represents a date time (timestamp)
	 */
	public boolean isOfDateTimeType()
	{
		return isOfSqlType(Types.TIMESTAMP);
	}
	
	/**
	 * @return If the type represents a date (date, timestamp)
	 */
	public boolean isOfDateType()
	{
		return isOfSqlType(Types.DATE) || isOfDateTimeType();
	}
	
	/**
	 * Parses an sql type from a string
	 * @param s A string that represents a data type. For example "varchar(32)"
	 * @return The data type represented by the string
	 * @see Types
	 */
	public static int parseSqlType(String s)
	{
		String lower = s.toLowerCase();
		
		if (lower.startsWith("bigint"))
			return Types.BIGINT;
		
		if (lower.startsWith("tiny"))
			return parseSqlType(s.substring(4));
		if (lower.startsWith("big"))
			return parseSqlType(s.substring(3));
		if (lower.startsWith("medium"))
			return parseSqlType(s.substring(6));
		if (lower.startsWith("small"))
			return parseSqlType(s.substring(5));
		if (lower.startsWith("long"))
			return parseSqlType(s.substring(4));
		
		if (lower.startsWith("varchar") || lower.startsWith("text"))
			return Types.VARCHAR;
		if (lower.startsWith("int"))
			return Types.INTEGER;
		if (lower.startsWith("boolean"))
			return Types.BOOLEAN;
		if (lower.startsWith("float"))
			return Types.FLOAT;
		if (lower.startsWith("decimal"))
			return Types.DECIMAL;
		if (lower.startsWith("double"))
			return Types.DOUBLE;
		if (lower.startsWith("char"))
			return Types.CHAR;
		if (lower.startsWith("timestamp") || lower.startsWith("datetime"))
			return Types.TIMESTAMP;
		if (lower.startsWith("date"))
			return Types.DATE;
		if (lower.startsWith("time"))
			return Types.TIME;
		if (lower.startsWith("blob"))
			return Types.VARBINARY;
		
		return Types.OTHER;
	}
	
	
	// SUBCLASSES	--------------------
	
	/**
	 * These exceptions are thrown when one tries to use invalid data types with attributes
	 * @author Mikko Hilpinen
	 * @since 17.9.2015
	 */
	public static class InvalidDataTypeException extends RuntimeException
	{
		// ATTRIBUTES	---------------
		
		private static final long serialVersionUID = -2413041704623953008L;
		private DataType usedType, correctType;
		private Object value;
		
		
		// CONSTRUCTOR	---------------
		
		/**
		 * Creates a new exception
		 * @param usedType The type that was being used
		 * @param correctType The correct type that should have been used
		 * @param value The value that was being used
		 */
		public InvalidDataTypeException(DataType usedType, DataType correctType, Object value)
		{
			super("Invalid data type used when setting " + 
					(value != null ? value.toString() : "null") + ". Type used: " + usedType + 
					", correct type: " + correctType);
			
			this.usedType = usedType;
			this.correctType = correctType;
			this.value = value;
		}
		
		
		// ACCESSORS	---------------
		
		/**
		 * @return The data type that was used
		 * @see Types
		 */
		public DataType getUsedType()
		{
			return this.usedType;
		}
		
		/**
		 * @return The correct data type that should be used
		 * @see Types
		 */
		public DataType getCorrectType()
		{
			return this.correctType;
		}
		
		/**
		 * @return the value that caused the exception
		 */
		public Object getValue()
		{
			return this.value;
		}
	}
}
