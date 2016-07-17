package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.vault.generics.Column;

/**
 * This where condition is used for checking whether values are in between each other
 * @author Mikko Hilpinen
 * @since 17.1.2016
 */
public class IsBetweenCondition extends SingleCondition
{
	// ATTRIBUTES	---------------
	
	private Column[] parts; // [0] BETWEEN [1] AND [2]
	
	
	// CONSTRUCTOR	---------------
	
	/**
	 * Creates a new between condition
	 * @param column The compared column
	 * @param minColumn The minimum value column
	 * @param maxColumn The maximum value column
	 * @param inverted Should the condition be inverted to "not between"
	 */
	public IsBetweenCondition(Column column, Column minColumn, 
			Column maxColumn, boolean inverted)
	{
		super(inverted);
		this.parts = new Column[] {column, minColumn, maxColumn};
	}
	
	/**
	 * Creates a new between condition
	 * @param value The compared value
	 * @param minColumn The minimum value column
	 * @param maxColumn The maximum value column
	 * @param inverted Should the condition be inverted to "not between"
	 */
	public IsBetweenCondition(Value value, Column minColumn, Column maxColumn, 
			boolean inverted)
	{
		super(inverted, value);
		this.parts = new Column[] {null, minColumn, maxColumn};
	}
	
	/**
	 * Creates a new between condition
	 * @param column The compared column
	 * @param minValue The minimum value
	 * @param maxValue The maximum value
	 * @param inverted Should the condition be inverted to "not between"
	 */
	public IsBetweenCondition(Column column, Value minValue, Value maxValue, 
			boolean inverted)
	{
		super(inverted, minValue, maxValue);
		this.parts = new Column[] {column, null, null};
	}
	
	/**
	 * Creates a new between condition
	 * @param column The compared column
	 * @param minColumn The minimum value column
	 * @param maxValue The maximum value
	 * @param inverted Should the condition be inverted to "not between"
	 */
	public IsBetweenCondition(Column column, Column minColumn, Value maxValue, 
			boolean inverted)
	{
		super(inverted, maxValue);
		this.parts = new Column[] {column, minColumn, null};
	}
	
	/**
	 * Creates a new between condition
	 * @param column The compared column
	 * @param minValue The minimum value
	 * @param maxColumn The maximum value column
	 * @param inverted Should the condition be inverted to "not between"
	 */
	public IsBetweenCondition(Column column, Value minValue, Column maxColumn, 
			boolean inverted)
	{
		super(inverted, minValue);
		this.parts = new Column[] {column, null, maxColumn};
	}
	
	
	// IMPLEMENTED METHODS	------

	@Override
	protected String getSQLWithPlaceholders()
	{
		// First sets the target data type
		specifyTargetType();
		
		// Then starts parsing the sql
		StringBuilder sql = new StringBuilder();
		appendPart(sql, 0);
		
		sql.append(" BETWEEN ");
		
		appendPart(sql, 1);
		sql.append(" AND ");
		appendPart(sql, 2);
		
		return sql.toString();
	}

	@Override
	protected String getDebugSqlWithNoParsing()
	{
		return getSQLWithPlaceholders();
	}

	
	// OTHER METHODS	----------
	
	private void specifyTargetType()
	{
		for (Column column : this.parts)
		{
			if (column != null)
			{
				specifyValueDataType(column.getType());
				break;
			}
		}
	}
	
	private void appendPart(StringBuilder sql, int index)
	{
		Column part = this.parts[index];
		if (part == null)
			sql.append("?");
		else
			sql.append(part.getColumnNameWithTable());
	}
}
