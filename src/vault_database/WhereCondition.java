package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Where conditions are used in database methods to limit the number of operated rows
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public abstract class WhereCondition
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
	public WhereCondition(int dataType, Object[] values, boolean inverted)
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
	 */
	protected abstract String getSQLWithPlaceholders(DatabaseTable targetTable);
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Creates an sql statement based on this where condition. The value spots will be marked 
	 * with '?' for future preparation
	 * @param targetTable The table the sql operation will be completed on
	 * @return A logical sql condition like 'NOT (columName = 0)'
	 */
	public String toSql(DatabaseTable targetTable)
	{
		StringBuilder sql = new StringBuilder();
		if (this.inverted)
			sql.append(" NOT");
		sql.append(" (");
		sql.append(getSQLWithPlaceholders(targetTable));
		sql.append(")");
		
		return sql.toString();
	}
	
	/**
	 * Sets condition values to the prepared sql statement
	 * @param statement The statement that is being prepared.
	 * @param startIndex The index where the first value of this condition occurs.
	 * @return The index of the next value insert
	 * @throws SQLException If the operation failed
	 */
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
	
	/**
	 * Returns a debug string that mimics the final sql statement created from this condition
	 * @param targetTable The table this condition is used in
	 * @return A debug sql statement
	 */
	public String getDebugSql(DatabaseTable targetTable)
	{
		String sql = toSql(targetTable);
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
	
	/**
	 * A where clause created from the two conditions
	 * @param other The other condition
	 * @return A combination of these two conditions
	 */
	public WhereClause and(WhereCondition other)
	{
		return toClause().and(other);
	}
	
	/**
	 * @return The where condition as a where clause
	 */
	public WhereClause toClause()
	{
		return new WhereClause(this);
	}
}
