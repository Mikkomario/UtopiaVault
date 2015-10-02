package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This where condition has only a single condition
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class SingleWhereCondition extends WhereCondition
{
	// ATTRIBUTES	------------------
	
	private int dataType;
	private boolean inverted;
	private Object[] values;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition
	 * @param dataType The data type used for the values
	 * @param values The values that are part of the condition
	 * @param inverted Should the condition be inverted
	 */
	public SingleWhereCondition(int dataType, boolean inverted, Object... values)
	{
		this.dataType = dataType;
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
	 * @return The data type for the condition's values
	 */
	protected int getDataType()
	{
		return this.dataType;
	}
	
	/**
	 * Updates the condition's data type
	 * @param newType The new data type for the condition values
	 */
	protected void updateDataType(int newType)
	{
		this.dataType = newType;
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
		for (Object value : this.values)
		{
			statement.setObject(i, value, this.dataType);
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
		for (Object value : this.values)
		{
			String valueString;
			if (value == null)
				valueString = "null";
			else
				valueString = "'" + value.toString() + "'";
			
			sql.replaceFirst("\\?", valueString);
		}
		
		return sql;
	}
}
