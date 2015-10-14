package vault_database;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import vault_database.DataType.InvalidDataTypeException;

/**
 * DatabaseValue is a bit more secure version of Object class to be used with database 
 * operations that are data type independent
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public class DatabaseValue
{
	// ATTRIBUTES	------------------
	
	private DataType type;
	private Object value;
	
	
	// CONSTRUCTOR	------------------
	
	private DatabaseValue(DataType dataType)
	{
		this.type = dataType;
		setToNull();
	}
	
	private DatabaseValue(DataType dataType, Object value)
	{
		this.type = dataType;
		setValue(value);
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
	
	/**
	 * Creates a new value from the string
	 * @param dataType The intended data type of the value
	 * @param string A string value
	 * @throws InvalidDataTypeException If the string can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, String string) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(string);
	}
	
	/**
	 * Creates a new value from the numeric double
	 * @param dataType The intended data type of the value
	 * @param numeric A numeric value
	 * @throws InvalidDataTypeException If the double can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, double numeric) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(numeric);
	}
	
	/**
	 * Creates a new value from the numeric long
	 * @param dataType The intended data type of the value
	 * @param numeric A numeric value
	 * @throws InvalidDataTypeException If the long can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, long numeric) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(numeric);
	}
	
	/**
	 * Creates a new value from the date value
	 * @param dataType The intended data type of the value
	 * @param date a date value
	 * @throws InvalidDataTypeException If the date can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, LocalDate date) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(date);
	}
	
	/**
	 * Creates a new value from the dateTime value
	 * @param dataType The intended data type of the value
	 * @param date a dateTime value
	 * @throws InvalidDataTypeException If the value can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, LocalDateTime date) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(date);
	}
	
	/**
	 * Creates a new value from the date value
	 * @param dataType The intended data type of the value
	 * @param date a date value
	 * @throws InvalidDataTypeException If the value can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, Date date) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(date);
	}
	
	/**
	 * Creates a new value from the timestamp value
	 * @param dataType The intended data type of the value
	 * @param date a timestamp value
	 * @throws InvalidDataTypeException If the value can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, Timestamp date) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(date);
	}
	
	/**
	 * Creates a new value from the boolean value
	 * @param dataType The intended data type of the value
	 * @param bool a boolean value
	 * @throws InvalidDataTypeException If the value can't be parsed to the given data type
	 */
	public DatabaseValue(DataType dataType, boolean bool) throws InvalidDataTypeException
	{
		this.type = dataType;
		setValue(bool);
	}
	
	/**
	 * Copies the value of another database value
	 * @param dataType The intended data type of the new value
	 * @param value a previous database value
	 * @throws InvalidDataTypeException If the value can't be casted to the intended data type
	 */
	public DatabaseValue(DataType dataType, DatabaseValue value) throws InvalidDataTypeException
	{
		this.type = dataType;
		if (getDataType().equals(value.getDataType()))
			this.value = value.getValue();
		else
			setValue(value.getValue());
	}
	
	/**
	 * Creates a new value from an object value
	 * @param dataType The intended data type of the value
	 * @param value The value in ambiguous object form
	 * @return A database value parsed from the object
	 * @throws InvalidDataTypeException If the object can't be parsed / cast to the intended data type
	 */
	public static DatabaseValue objectValue(DataType dataType, Object value) throws InvalidDataTypeException
	{
		return new DatabaseValue(dataType, value);
	}
	
	/**
	 * Creates a null value
	 * @param dataType The data type of the attribute / value
	 * @return A null database value
	 */
	public static DatabaseValue nullValue(DataType dataType)
	{
		return new DatabaseValue(dataType);
	}
	
	/**
	 * Creates a string value
	 * @param string The value
	 * @return A string value
	 */
	public static DatabaseValue String(String string)
	{
		return new DatabaseValue(DataType.STRING, string);
	}
	
	/**
	 * Creates a double value
	 * @param number the value
	 * @return a double value
	 */
	public static DatabaseValue Double(double number)
	{
		return new DatabaseValue(DataType.DOUBLE, number);
	}
	
	/**
	 * Creates a long value
	 * @param number The value
	 * @return A long type value
	 */
	public static DatabaseValue Long(long number)
	{
		return new DatabaseValue(DataType.LONG, number);
	}
	
	/**
	 * Creates an integer value
	 * @param number the value
	 * @return an integer type value
	 */
	public static DatabaseValue Integer(int number)
	{
		return new DatabaseValue(DataType.INTEGER, number);
	}
	
	/**
	 * Creates a date value
	 * @param date the value
	 * @return a date type value
	 */
	public static DatabaseValue Date(LocalDate date)
	{
		return new DatabaseValue(DataType.DATE, date);
	}
	
	/**
	 * Creates a date time value
	 * @param date the value
	 * @return a dateTime type value
	 */
	public static DatabaseValue DateTime(LocalDateTime date)
	{
		return new DatabaseValue(DataType.DATETIME, date);
	}
	
	/**
	 * creates a boolean value
	 * @param bool the value
	 * @return a boolean type value
	 */
	public static DatabaseValue Boolean(boolean bool)
	{
		return new DatabaseValue(DataType.BOOLEAN, bool);
	}
	
	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toString()
	{
		if (isNull())
			return null;
		if (getDataType().isOfStringType())
			return getValue().toString();
		if (getDataType().isOfDateTimeType())
			return toDateTime().toString();
		if (getDataType().isOfDateType())
			return toLocalDate().toString();
		if (getDataType().isOfLongType())
			return toLong() + "";
		if (getDataType().isOfIntegerType())
			return toInt() + "";
		if (getDataType().isOfNumericType())
			return toDouble() + "";
		if (getDataType().isOfBooleanType())
			return (toBoolean() ? "true" : "false");
		
		return getValue().toString();
	}

	
	// ACCESSORS	------------------
	
	/**
	 * @return The (intended) data type of this value
	 */
	public DataType getDataType()
	{
		return this.type;
	}
	
	private Object getValue()
	{
		return this.value;
	}
	
	/**
	 * Changes the attribute's value. Can't check data types. Invalid type may cause problems 
	 * later.
	 * @param object The attribute's new value.
	 */
	private void setValue(Object object)
	{
		if (object == null)
			setToNull();
		else if (object instanceof DatabaseValue)
			setValue(((DatabaseValue) object).toObject());
		else if (object instanceof LocalDateTime)
			setValue((LocalDateTime) object);
		else if (object instanceof LocalDate)
			setValue((LocalDate) object);
		else if (object instanceof Date)
			setValue((Date) object);
		else if (object instanceof Timestamp)
			setValue((Timestamp) object);
		else if (object instanceof Number)
		{
			Number number = (Number) object;
			if (getDataType().isOfLongType())
				setValue(number.longValue());
			else
				setValue(number.doubleValue());
		}
		else if (object instanceof Boolean)
			setValue(((Boolean) object).booleanValue());
		else if (object instanceof String)
			setValue((String) object);
		else if (getDataType().isOfStringType())
			setValue(object.toString());
		else
			this.value = object;
	}
	
	private void setValue(String string) throws InvalidDataTypeException
	{
		if (string == null)
			setToNull();
		else if (getDataType().isOfDateTimeType())
			setValue(valueAsDateTime(string));
		else if (getDataType().isOfDateType())
			setValue(valueAsDate(string));
		else if (getDataType().isOfLongType())
			setValue(valueAsLong(string));
		else if (getDataType().isOfIntegerType())
			setValue(valueAsInt(string));
		else if (getDataType().isOfNumericType())
			setValue(valueAsDouble(string));
		else if (getDataType().isOfBooleanType())
			setValue(Boolean.parseBoolean(string));
		else if (getDataType().isOfStringType())
			this.value = string;
		else
			throw getDataInsertException(DataType.STRING, string);
	}
	
	private void setValue(double numeric)
	{
		if (getDataType().isOfLongType())
			this.value = (long) numeric;
		else if (getDataType().isOfIntegerType())
			this.value = (int) numeric;
		else if (getDataType().isOfNumericType())
			this.value = numeric;
		else if (getDataType().isOfBooleanType())
			this.value = (numeric != 0);
		else if (getDataType().isOfStringType())
			this.value = numeric + "";
		else
			throw getDataInsertException(DataType.DOUBLE, numeric);
	}
	
	private void setValue(long numeric)
	{
		if (getDataType().isOfLongType())
			this.value = numeric;
		else if (getDataType().isOfIntegerType())
			this.value = (int) numeric;
		else if (getDataType().isOfNumericType())
			this.value = (double) numeric;
		else if (getDataType().isOfBooleanType())
			this.value = (numeric != 0);
		else if (getDataType().isOfStringType())
			this.value = numeric + "";
		else
			throw getDataInsertException(DataType.LONG, numeric);
	}
	
	private void setValue(LocalDate date)
	{
		if (date == null)
			setToNull();
		else if (getDataType().isOfDateTimeType())
			throw getDataInsertException(DataType.DATE, date);
		else if (getDataType().isOfDateType())
			this.value = Date.valueOf(date);
		else if (getDataType().isOfStringType())
			setValue(date.toString());
		else
			throw getDataInsertException(DataType.DATE, date);
	}
	
	private void setValue(Date date)
	{
		if (date == null)
			setToNull();
		else if (getDataType().isOfDateTimeType())
			throw getDataInsertException(DataType.DATE, date);
		else if (getDataType().isOfDateType())
			this.value = date;
		else if (getDataType().isOfStringType())
			setValue(date.toLocalDate().toString());
		else
			throw getDataInsertException(DataType.DATE, date);
	}
	
	private void setValue(LocalDateTime date)
	{
		if (date == null)
			setToNull();
		else if (getDataType().isOfDateTimeType())
			this.value = Timestamp.valueOf(date);
		else if (getDataType().isOfDateType())
			setValue(date.toLocalDate());
		else if (getDataType().isOfStringType())
			setValue(date.toString());
		else
			throw getDataInsertException(DataType.DATETIME, date);
	}
	
	private void setValue(Timestamp date) throws InvalidDataTypeException
	{
		if (date == null)
			setToNull();
		else if (getDataType().isOfDateTimeType())
			this.value = date;
		else if (getDataType().isOfDateType())
			setValue(date.toLocalDateTime().toLocalDate());
		else if (getDataType().isOfStringType())
			setValue(date.toLocalDateTime().toString());
		else
			throw getDataInsertException(DataType.DATETIME, date);
	}
	
	private void setValue(boolean bool) throws InvalidDataTypeException
	{
		if (getDataType().isOfNumericType())
			setValue(bool ? 1 : 0);
		else if (getDataType().isOfBooleanType())
			this.value = bool;
		else if (getDataType().isOfStringType())
			setValue(bool ? "true" : "false");
		else
			throw getDataInsertException(DataType.BOOLEAN, bool);
	}
	
	
	
	
	// OTHER METHODS	-------------
	
	/**
	 * @return A description of this value, including the data type. May be something like 
	 * '3 (int)' or 'test (string)'
	 */
	public String getDescription()
	{
		StringBuilder s = new StringBuilder();
		if (isNull())
			s.append("null");
		else
			s.append(toString());
		
		s.append(" (");
		s.append(getDataType());
		s.append(")");
		
		return s.toString();
	}
	
	/**
	 * Prepares the database value into a prepared statement
	 * @param statement The statement that is being prepared
	 * @param index The index of the inserted value (starting from 1)
	 * @throws SQLException If an sql exception occurred
	 */
	public void setToStatement(PreparedStatement statement, int index) throws SQLException
	{
		if (isNull())
			statement.setNull(index, getDataType().toSqlType());
		else
			statement.setObject(index, getValue(), getDataType().toSqlType());
	}
	
	/**
	 * @return The uncasted attribute value
	 */
	public Object toObject()
	{
		return getValue();
	}
	
	/**
	 * @return Is the attribute's current value set to null
	 */
	public boolean isNull()
	{
		return this.value == null;
	}
	
	/**
	 * Checks if the two database values are equal. To be equal, the values need to be of the 
	 * same data type and have the same value. For string types, the check is case-insensitive.
	 * @param other The other value
	 * @return Are the two values equal
	 */
	public boolean equals(DatabaseValue other)
	{		
		// Checks the data types
		if (!getDataType().equals(other.getDataType()))
			return false;
		
		// Checks for null values
		if (isNull())
			return other.isNull();
		else if (other.isNull())
			return false;
		
		// Checks for case-insensitive strings
		if (getDataType().isOfStringType())
			return toString().equalsIgnoreCase(other.toString());
		// Otherwise uses object values
		else
			return toObject().equals(other.toObject());
	}
	
	private void setToNull()
	{
		this.value = null;
	}
	
	/**
	 * @return The value as a long
	 * @throws InvalidDataTypeException if the value can't be parsed to long
	 */
	public long toLong()
	{
		if (isNull())
			return 0;
		if (getDataType().isOfNumericType())
			return ((Number) getValue()).longValue();
		if (getDataType().isOfStringType())
			return valueAsLong(toString());
		if (getDataType().isOfBooleanType())
			return toBoolean() ? 1 : 0;
		throw getDataCastException(DataType.LONG);
	}
	
	/**
	 * @return The value as an integer
	 * @throws InvalidDataTypeException If the value can't be parsed to an integer
	 */
	public int toInt() throws InvalidDataTypeException
	{
		if (isNull())
			return 0;
		if (getDataType().isOfNumericType())
			return ((Number) getValue()).intValue();
		if (getDataType().isOfStringType())
			return valueAsInt(toString());
		if (getDataType().isOfBooleanType())
			return toBoolean() ? 1 : 0;
		throw getDataCastException(DataType.INTEGER);
	}
	
	/**
	 * @return The value as a date
	 * @throws InvalidDataTypeException If the value can't be parsed to localDate
	 */
	public LocalDate toLocalDate() throws InvalidDataTypeException
	{
		if (isNull())
			return null;
		if (getDataType().isOfDateTimeType())
			return toDateTime().toLocalDate();
		if (getDataType().isOfDateType())
			return toDate().toLocalDate();
		if (getDataType().isOfStringType())
			return valueAsDate(toString());
		throw getDataCastException(DataType.DATE);
	}
	
	/**
	 * @return The value as a date
	 * @throws InvalidDataTypeException If the value can't be parsed to localDate
	 */
	public Date toDate() throws InvalidDataTypeException
	{
		if (isNull())
			return null;
		if (getDataType().isOfDateTimeType())
			return new Date(toTimestamp().getTime());
		if (getDataType().isOfDateType())
			return ((Date) getValue());
		if (getDataType().isOfStringType())
			return Date.valueOf(valueAsDate(toString()));
		throw getDataCastException(DataType.DATE);
	}
	
	/**
	 * @return The value as a dateTime
	 * @throws InvalidDataTypeException If the value can't be parsed to dateTime
	 */
	public LocalDateTime toDateTime() throws InvalidDataTypeException
	{
		if (isNull())
			return null;
		if (getDataType().isOfDateTimeType())
			return toTimestamp().toLocalDateTime();
		if (getDataType().isOfStringType())
			return valueAsDateTime(toString());
		throw getDataCastException(DataType.DATETIME);
	}
	
	/**
	 * @return The value as a timestamp
	 * @throws InvalidDataTypeException If the value can't be parsed to timestamp
	 */
	public Timestamp toTimestamp() throws InvalidDataTypeException
	{
		if (isNull())
			return null;
		if (getDataType().isOfDateTimeType())
			return (Timestamp) getValue();
		if (getDataType().isOfStringType())
			return Timestamp.valueOf(valueAsDateTime(toString()));
		throw getDataCastException(DataType.DATETIME);
	}
	
	/**
	 * @return The attribute value as a boolean
	 * @throws InvalidDataTypeException If the attribute isn't of boolean or tinyint type
	 */
	public boolean toBoolean() throws InvalidDataTypeException
	{
		if (isNull())
			return false;
		if (getDataType().isOfLongType())
			return toLong() != 0;
		if (getDataType().isOfNumericType())
			return toInt() != 0;
		if (getDataType().isOfBooleanType())
			return (boolean) getValue();
		if (getDataType().isOfStringType())
			return Boolean.parseBoolean(toString());
		throw getDataCastException(DataType.BOOLEAN);
	}
	
	/**
	 * @return The value as a double
	 * @throws InvalidDataTypeException If the value can't be parsed to double
	 */
	public double toDouble() throws InvalidDataTypeException
	{
		if (isNull())
			return 0;
		if (getDataType().isOfNumericType())
			return ((Number) getValue()).doubleValue();
		if (getDataType().isOfBooleanType())
			return toBoolean() ? 1 : 0;
		if (getDataType().isOfStringType())
			return valueAsDouble(toString());
		throw getDataCastException(DataType.DOUBLE);
	}
	
	private static long valueAsLong(String value)
	{
		try
		{
			return Long.parseLong(value);
		}
		catch (NumberFormatException e)
		{
			throw getDataCastException(DataType.STRING, DataType.LONG, value);
		}
	}
	
	private static int valueAsInt(String value) throws InvalidDataTypeException
	{
		try
		{
			return Integer.parseInt(value);
		}
		catch (NumberFormatException e)
		{
			throw getDataCastException(DataType.STRING, DataType.INTEGER, value);
		}
	}
	
	private static double valueAsDouble(String value) throws InvalidDataTypeException
	{
		try
		{
			return Double.parseDouble(value);
		}
		catch (NumberFormatException e)
		{
			throw getDataCastException(DataType.STRING, DataType.DOUBLE, value);
		}
	}
	
	private static LocalDate valueAsDate(String value) throws InvalidDataTypeException
	{
		try
		{
			return LocalDate.parse(value);
		}
		catch (DateTimeParseException e)
		{
			try
			{
				return Date.valueOf(value).toLocalDate();
			}
			catch (IllegalArgumentException e1)
			{
				throw getDataCastException(DataType.STRING, DataType.DATE, value);
			}
		}
	}
	
	private static LocalDateTime valueAsDateTime(String value) throws InvalidDataTypeException
	{
		if (value.equalsIgnoreCase("CURRENT_TIMESTAMP"))
			return LocalDateTime.now();
		
		try
		{
			return LocalDateTime.parse(value);
		}
		catch (DateTimeParseException e)
		{
			try
			{
				return Timestamp.valueOf(value).toLocalDateTime();
			}
			catch (IllegalArgumentException e1)
			{
				throw getDataCastException(DataType.STRING, DataType.DATETIME, value);
			}
		}
	}
	
	private InvalidDataTypeException getDataInsertException(DataType usedDataType, 
			Object insertedValue)
	{
		return new InvalidDataTypeException(usedDataType, getDataType(), insertedValue);
	}
	
	private static InvalidDataTypeException getDataCastException(DataType fromCast, DataType toCast, 
			Object casted)
	{
		return new InvalidDataTypeException(fromCast, toCast, casted);
	}
	
	private InvalidDataTypeException getDataCastException(DataType correctDataType)
	{
		return new InvalidDataTypeException(getDataType(), correctDataType, getValue());
	}
}
