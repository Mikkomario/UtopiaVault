package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import vault_generics.SqlDataType;
import flow_generics.DataTypeException;
import flow_generics.Value;

/**
 * This where condition has only a single condition
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class SingleWhereCondition extends WhereCondition
{
	// ATTRIBUTES	------------------
	
	private boolean inverted;
	private Value[] values;
	private SqlDataType targetType = null;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition
	 * @param values The value(s) used in the condition (optional)
	 * @param inverted Should the condition be inverted
	 */
	public SingleWhereCondition(boolean inverted, Value... values)
	{
		this.values = values;
		this.inverted = inverted;
	}
	
	/**
	 * Creates a new where condition
	 * @param values The value(s) used in the condition (optional)
	 */
	public SingleWhereCondition(Value... values)
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
	 * @throws WhereConditionParseException If the parsing fails
	 */
	protected abstract String getSQLWithPlaceholders() throws 
			WhereConditionParseException;
	
	/**
	 * This method is used when creating a debug message for a where condition that couldn't 
	 * be parsed.
	 * @return A debug message describing the where condition. No parsing should be done.
	 */
	protected abstract String getDebugSqlWithNoParsing();
	
	
	// IMPLEMENTED METHODS	--------
	
	@Override
	public String toSql() throws WhereConditionParseException
	{
		if (!this.inverted)
			return getSQLWithPlaceholders();
		
		StringBuilder sql = new StringBuilder(" NOT (");
		sql.append(getSQLWithPlaceholders());
		sql.append(")");
		
		return sql.toString();
	}
	
	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex) throws 
			SQLException, WhereConditionParseException
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
				
				statement.setObject(startIndex, castValue.getObjectValue(), targetType.getSqlType());
				index ++;
			}
			catch (DataTypeException e)
			{
				throw new WhereConditionParseException("Failed to cast " + 
						value.getDescription() + " to a compatible data type", e);
			}
		}
		
		return index;
	}
	
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
				
				sql.replaceFirst("\\?", valueString);
			}
			
			return sql;
		}
		catch (WhereConditionParseException e)
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
	
	
	// ACCESSORS	-----------------
	
	/**
	 * @return The value(s) used in the condition
	 */
	protected Value[] getValues()
	{
		return this.values;
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Specifies the data type used with the provided value
	 * @param type The data type used for the condition's value
	 */
	protected void specifyValueDataType(SqlDataType type)
	{
		this.targetType = type;
	}
}
