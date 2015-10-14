package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import vault_database.DataType.InvalidDataTypeException;

/**
 * This where condition has only a single condition
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class SingleWhereCondition extends WhereCondition
{
	// ATTRIBUTES	------------------
	
	private boolean inverted;
	private DatabaseValue[] values;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition
	 * @param values The values that are part of the condition
	 * @param inverted Should the condition be inverted
	 */
	public SingleWhereCondition(boolean inverted, DatabaseValue... values)
	{
		this.values = values;
		this.inverted = inverted;
	}
	
	
	// ABSTRACT METHODS	--------------
	
	/**
	 * In this method the subclass should return a string that represents the where condition 
	 * in sql format. The placeholders for values should be marked with '?'
	 * @param targetTable The table the operation will be completed on
	 * @return A logical sql condition with a boolean return value and '?' as placeholders 
	 * for the values
	 * @throws WhereConditionParseException If the parsing fails
	 */
	protected abstract String getSQLWithPlaceholders(DatabaseTable targetTable) throws 
			WhereConditionParseException;
	
	
	// ACCESSORS	-----------------
	
	/**
	 * @return The currently used values
	 */
	protected DatabaseValue[] getValues()
	{
		return this.values;
	}
	
	/**
	 * Updates the currently used values. May be used by subclasses to change data types etc.
	 * @param values The new value set
	 */
	protected void updateValues(DatabaseValue[] values)
	{
		this.values = values;
	}
	
	/**
	 * Casts all of the provided values to a certain data type
	 * @param desiredType The type the values will be casted to
	 * @throws WhereConditionParseException If the casting fails at some point
	 */
	protected void castValuesToDataType(DataType desiredType) throws WhereConditionParseException
	{
		// Parses the index to the correct type
		try
		{
			DatabaseValue[] updatedValues = new DatabaseValue[getValues().length];
			for (int i = 0; i < updatedValues.length; i++)
			{
				updatedValues[i] = new DatabaseValue(desiredType, getValues()[i]);
			}
			updateValues(updatedValues);
		}
		catch (InvalidDataTypeException e)
		{
			throw new WhereConditionParseException(
					"The provided index can't be parsed to the desired data type (" + 
					desiredType + ")", e);
		}
	}
	
	
	// OTHER METHODS	--------------
	
	@Override
	public String toSql(DatabaseTable targetTable) throws WhereConditionParseException
	{
		if (!this.inverted)
			return getSQLWithPlaceholders(targetTable);
		
		StringBuilder sql = new StringBuilder(" NOT (");
		sql.append(getSQLWithPlaceholders(targetTable));
		sql.append(")");
		
		return sql.toString();
	}
	
	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex) throws SQLException
	{
		int i = startIndex;
		for (DatabaseValue value : this.values)
		{
			value.setToStatement(statement, i);
			i ++;
		}
		
		return i;
	}
	
	@Override
	public String getDebugSql(DatabaseTable targetTable)
	{
		String sql;
		try
		{
			sql = toSql(targetTable);
		}
		catch (WhereConditionParseException e)
		{
			return "Can't be parsed";
		}
		for (DatabaseValue value : this.values)
		{
			String valueString;
			if (value == null)
				valueString = "null";
			else
				valueString = "'" + value.toString() + "'";
			
			sql = sql.replaceFirst("\\?", valueString);
		}
		
		return sql;
	}
}