package utopia.vault.generics;

import java.sql.Date;
import java.sql.Timestamp;

import utopia.flow.generics.DataType;
import utopia.flow.generics.DataTypeException;
import utopia.flow.generics.Value;
import utopia.flow.generics.Variable;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * Database variables are variables that are tied to certain columns
 * @author Mikko Hilpinen
 * @since 9.1.2015
 */
public class ColumnVariable extends Variable
{
	// ATTRIBUTES	-----------------
	
	private Column column;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a copy of another database variable
	 * @param other
	 */
	public ColumnVariable(ColumnVariable other)
	{
		super(other);
		
		this.column = other.getColumn();
	}
	
	/**
	 * Creates a database variable based on another variable
	 * @param table A table that should contain the variable's column
	 * @param other another variable
	 */
	public ColumnVariable(Variable other, Table table)
	{
		super(other);
		
		if (other instanceof ColumnVariable)
			this.column = ((ColumnVariable) other).getColumn();
		else
			this.column = table.findColumnWithVariableName(getName());
		
		// Checks for nulls
		setValue(getValue());
	}

	/**
	 * Creates a new database variable
	 * @param column The column the variable is based on
	 * @param value The value assigned to the variable
	 */
	public ColumnVariable(Column column, Value value)
	{
		super(column.getName(), column.getType(), handleNotNull(column, value));
		this.column = column;
	}

	/**
	 * Creates a new database variable with the default value
	 * @param column the column this variable is based on
	 */
	public ColumnVariable(Column column)
	{
		super(column.getName(), column.getType(), column.getDefaultValue());
		this.column = column;
	}

	/**
	 * Creates a new database variable with the provided object value. The value will be 
	 * casted to the correct type
	 * @param column The column this variable is based on
	 * @param value The object value assigned to the variable
	 * @param valueType The data type of the provided value
	 * @throws DataTypeException If the casting failed
	 */
	public ColumnVariable(Column column, Object value, DataType valueType) throws DataTypeException
	{
		super(column.getName(), column.getType(), handleNotNull(column, 
				new Value(value, valueType)));
		this.column = column;
	}
	
	/**
	 * Creates a new database variable
	 * @param table The table the variable's column should be at
	 * @param variableName The name of the variable
	 * @param value The value that will be assigned to the variable
	 * @return A new database variable
	 * @throws NoSuchColumnException If the table doesn't contain a column for the variable
	 */
	public static ColumnVariable createVariable(Table table, String variableName, 
			Value value) throws NoSuchColumnException
	{
		Column column = table.findColumnWithVariableName(variableName);
		return new ColumnVariable(column, value);
	}
	
	
	// IMPLEMENTED METHODS	---------
	
	/**
	 * Changes the variable's value. If the assigned value is null and the column doesn't allow 
	 * null values and it has a specified default value, the default value is used.
	 */
	@Override
	public void setValue(Value value)
	{
		super.setValue(handleNotNull(getColumn(), value));
	}

	
	// ACCESSORS	-----------------
	
	/**
	 * @return The column this variable uses
	 */
	public Column getColumn()
	{
		return this.column;
	}
	
	
	// OTHER METHODS	------------
	
	/**
	 * @return The variable's contents as a sql date
	 */
	public Date getSqlDateValue()
	{
		return BasicSqlDataType.valueToDate(getValue());
	}
	
	/**
	 * @return The variable's contents as a timestamp
	 */
	public Timestamp getTimestampValue()
	{
		return BasicSqlDataType.valueToTimeStamp(getValue());
	}
	
	private static Value handleNotNull(Column column, Value value)
	{
		// Null values are replaced with default values where the column wouldn't otherwise 
		// allow null
		if (column.nullAllowed())
			return value;
		else if (value.isNull())
		{
			Value defaultValue = column.getDefaultValue();
			if (defaultValue == null || defaultValue.isNull())
				return value;//throw new NullNotAllowedException(column);
			else
				return defaultValue;
		}
		else
			return value;
	}
	
	
	// NESTED CLASSES	-------------------
	
	/*
	private static class NullNotAllowedException extends RuntimeException
	{
		private static final long serialVersionUID = -4316410740660035059L;

		public NullNotAllowedException(Column column)
		{
			super("Column " + column.getColumnName() + " doesn't allow null values");
		}
	}*/
}
