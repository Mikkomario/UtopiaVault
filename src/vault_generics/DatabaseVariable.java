package vault_generics;

import java.sql.Date;
import java.sql.Timestamp;

import flow_generics.DataType;
import flow_generics.DataTypeException;
import flow_generics.Value;
import flow_generics.Variable;

/**
 * Database variables are variables that are tied to certain columns
 * @author Mikko Hilpinen
 * @since 9.1.2015
 */
public class DatabaseVariable extends Variable
{
	// ATTRIBUTES	-----------------
	
	private Column column;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a copy of another database variable
	 * @param other
	 */
	public DatabaseVariable(DatabaseVariable other)
	{
		super(other);
		
		this.column = other.getColumn();
	}
	
	/**
	 * Creates a database variable based on another variable
	 * @param table A table that should contain the variable's column
	 * @param other another variable
	 */
	public DatabaseVariable(Variable other, DatabaseTable table)
	{
		super(other);
		
		if (other instanceof DatabaseVariable)
			this.column = ((DatabaseVariable) other).getColumn();
		else
			this.column = table.findColumnWithVariableName(getName());
	}

	/**
	 * Creates a new database variable
	 * @param column The column the variable is based on
	 * @param value The value assigned to the variable
	 */
	public DatabaseVariable(Column column, Value value)
	{
		super(column.getName(), column.getType(), value);
		this.column = column;
	}

	/**
	 * Creates a new database variable with the default value
	 * @param column the column this variable is based on
	 */
	public DatabaseVariable(Column column)
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
	public DatabaseVariable(Column column, Object value, DataType valueType) throws DataTypeException
	{
		super(column.getName(), column.getType(), value, valueType);
		this.column = column;
	}
	
	/**
	 * Creates a new database variable
	 * @param table The table the variable's column should be at
	 * @param variableName The name of the variable
	 * @param value The value that will be assigned to the variable
	 * @return A new database variable
	 */
	public static DatabaseVariable createVariable(DatabaseTable table, String variableName, 
			Value value)
	{
		Column column = table.findColumnWithVariableName(variableName);
		return new DatabaseVariable(column, value);
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
		return SimpleSqlDataType.valueToDate(getValue());
	}
	
	/**
	 * @return The variable's contents as a timestamp
	 */
	public Timestamp getTimestampValue()
	{
		return SimpleSqlDataType.valueToTimeStamp(getValue());
	}
}
