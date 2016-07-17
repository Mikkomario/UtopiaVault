package utopia.vault.database;

import utopia.flow.generics.DataType;
import utopia.flow.generics.Value;

/**
 * This condition has only a single condition
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class SingleCondition extends Condition
{
	// ATTRIBUTES	------------------
	
	private boolean inverted; // TODO: Move the invertion into a separate condition class
	private Value[] values;
	private DataType targetType = null;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition
	 * @param values The value(s) used in the condition (optional)
	 * @param inverted Should the condition be inverted
	 */
	public SingleCondition(boolean inverted, Value... values)
	{
		this.values = values;
		this.inverted = inverted;
	}
	
	/**
	 * Creates a new where condition
	 * @param values The value(s) used in the condition (optional)
	 */
	public SingleCondition(Value... values)
	{
		this.values = values;
		this.inverted = false;
	}
	
	
	// ABSTRACT METHODS	--------------
	
	/**
	 * In this method the subclass should return a string that represents the where condition 
	 * in sql format. The place holders for values should be marked with '?'
	 * @return A logical sql condition with a boolean return value and '?' as place holders 
	 * for the values
	 * @throws StatementParseException If the parsing fails
	 */
	protected abstract String getSQLWithPlaceholders() throws 
			StatementParseException;
	
	/**
	 * This method is used when creating a debug message for a where condition that couldn't 
	 * be parsed.
	 * @return A debug message describing the where condition. No parsing should be done.
	 */
	protected abstract String getDebugSqlWithNoParsing();
	
	
	// IMPLEMENTED METHODS	--------
	
	/**
	 * @return The value(s) used in the condition. The returned array is a copy and changes 
	 * made to it won't affect the condition.
	 */
	@Override
	public Value[] getValues()
	{
		// If a specific data type has been specified, the values are casted first
		if (this.targetType == null)
			return this.values.clone();
		else
		{
			Value[] castValues = new Value[this.values.length];
			for (int i = 0; i < castValues.length; i++)
			{
				castValues[i] = this.values[i].castTo(this.targetType);
			}
			
			return castValues;
		}
	}
	
	@Override
	public String toSql() throws StatementParseException
	{
		if (!this.inverted)
			return getSQLWithPlaceholders();
		
		StringBuilder sql = new StringBuilder(" NOT (");
		sql.append(getSQLWithPlaceholders());
		sql.append(")");
		
		return sql.toString();
	}
	
	/*
	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex) throws 
			SQLException, StatementParseException
	{
		// If there is no value, there is no insert
		int index = startIndex;
		for (Value value : this.values)
		{
			try
			{
				// Casts the value first
				Value castValue;
				SqlDataType targetType;
				
				if (this.targetType != null)
				{
					castValue = value.castTo(this.targetType);
					targetType = this.targetType;
				}
				else
				{
					castValue = SqlDataType.castToSqlType(value);
					targetType = SqlDataType.castToSqlDataType(castValue.getType());
				}
				
				statement.setObject(index, castValue.getObjectValue(), targetType.getSqlType());
				index ++;
			}
			catch (DataTypeException e)
			{
				throw new StatementParseException("Failed to cast " + 
						value.getDescription() + " to a compatible data type", e);
			}
		}
		
		return index;
	}
	*/
	
	@Override
	public String getDebugSql()
	{
		try
		{
			String sql = toSql();
			for (Value value : this.values)
			{
				String valueString = value.toString();
				if (valueString == null)
					valueString = "null";
				else
					valueString = "'" + valueString + "'";
				
				sql = sql.replaceFirst("\\?", valueString);
			}
			
			return sql;
		}
		catch (StatementParseException e)
		{
			StringBuilder s = new StringBuilder();
			if (this.inverted)
			{
				s.append("NOT (");
				s.append(getDebugSqlWithNoParsing());
				s.append(")");
			}
			else
				s.append(getDebugSqlWithNoParsing());
			
			if (this.values.length == 0)
				s.append("\n No values used");
			else
			{
				s.append("\nValues used:");
				for (Value value : this.values)
				{
					s.append(value.getDescription());
				}
			}
			
			return s.toString();
		}
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Specifies the data type used with the provided value(s)
	 * @param type The data type used for the condition's value(s)
	 */
	protected void specifyValueDataType(DataType type)
	{
		this.targetType = type;
	}
}
