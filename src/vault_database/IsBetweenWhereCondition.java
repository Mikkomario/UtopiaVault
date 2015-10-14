package vault_database;

import vault_database.DatabaseTable.Column;

/**
 * This where condition checks if a value is between two other values
 * @author Mikko Hilpinen
 * @since 5.10.2015
 */
public class IsBetweenWhereCondition extends SingleWhereCondition
{
	// ATTRIBUTES	-----------------
	
	private String[] columnNames;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a new where condition where the value of the provided column must be between 
	 * the two provided values
	 * @param inverted Should the statement be inverted to 'not between'
	 * @param columnName The name of the column
	 * @param betweenFirst The first value
	 * @param betweenSecond The second value
	 */
	public IsBetweenWhereCondition(boolean inverted, String columnName, 
			DatabaseValue betweenFirst, DatabaseValue betweenSecond)
	{
		super(inverted, betweenFirst, betweenSecond);
		
		this.columnNames = new String[1];
		this.columnNames[0] = columnName;
	}
	
	/**
	 * Creates a new where condition where the provided value must be between two column 
	 * values
	 * @param inverted Should the statement be inverted to 'not between'
	 * @param value The value
	 * @param betweenFirstColumnName The first column name
	 * @param betweenSecondColumnName The second column name
	 */
	public IsBetweenWhereCondition(boolean inverted, DatabaseValue value, 
			String betweenFirstColumnName, String betweenSecondColumnName)
	{
		super(inverted, value);
		
		this.columnNames = new String[2];
		this.columnNames[0] = betweenFirstColumnName;
		this.columnNames[1] = betweenSecondColumnName;
	}
	
	/**
	 * Creates a new where condition where the provided column value must be between the 
	 * two other column values
	 * @param inverted Should the statement be inverted to 'not between'
	 * @param columnName The name of the first column
	 * @param betweenFirstColumnName The name of the second column
	 * @param betweenSecondColumnName The name of the third column
	 */
	public IsBetweenWhereCondition(boolean inverted, String columnName, 
			String betweenFirstColumnName, String betweenSecondColumnName)
	{
		super(inverted);
		
		this.columnNames = new String[3];
		this.columnNames[0] = columnName;
		this.columnNames[1] = betweenFirstColumnName;
		this.columnNames[2] = betweenSecondColumnName;
	}
	
	
	// IMPLEMENTED METHODS	---------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable)
			throws WhereConditionParseException
	{
		// Finds out the correct data type (if possible)
		DataType desiredType = null;
		for (String columnName : this.columnNames)
		{
			Column column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
					columnName);
			if (column != null)
			{
				desiredType = column.getType();
				break;
			}
		}
		
		// Casts the values (if possible)
		if (desiredType != null)
			castValuesToDataType(desiredType);
		
		// Parses the sql
		StringBuilder sql = new StringBuilder();
		if (this.columnNames.length != 2)
			sql.append(this.columnNames[0]);
		else
			sql.append("?");
		
		sql.append(" BETWEEN ");
		
		if (this.columnNames.length < 2)
			sql.append("? AND ?");
		else if (this.columnNames.length == 2)
			sql.append(this.columnNames[0] + " AND" + this.columnNames[1]);
		else
			sql.append(this.columnNames[1] + " AND " + this.columnNames[2]);
		
		
		return sql.toString();
	}
}
