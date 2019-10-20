package utopia.vault.database;

import utopia.flow.generics.DataType;
import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;

/**
 * This condition has only a single condition
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class SingleCondition extends Condition
{
	// ATTRIBUTES	------------------
	
	private ImmutableList<Value> values;
	private DataType targetType = null;
	private boolean valuesCasted = false;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition
	 * @param values The value(s) used in the condition (optional)
	 */
	public SingleCondition(Value... values)
	{
		this.values = ImmutableList.of(values);
	}
	
	/**
	 * Creates a new where condition
	 * @param values The values used in the condition
	 */
	public SingleCondition(ImmutableList<Value> values)
	{
		this.values = values;
	}
	
	
	// ABSTRACT METHODS	--------------
	
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
	public ImmutableList<Value> getValues()
	{
		// If a specific data type has been specified, the values are casted first
		if (this.targetType != null && !this.valuesCasted)
		{
			this.values = this.values.map(value -> value.castTo(this.targetType));
			this.valuesCasted = true;
		}
		
		return this.values;
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
	
	/*
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
			StringBuilder s = new StringBuilder(getDebugSqlWithNoParsing());
			
			if (this.values.isEmpty())
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
	*/
	
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
