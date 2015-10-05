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
	
	private String columnName;
	
	
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
		
		this.columnName = columnName;
	}
	
	
	// IMPLEMENTED METHODS	---------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable)
			throws WhereConditionParseException
	{
		// Finds out the correct data type
		Column column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
				this.columnName);
		if (column == null)
			throw new WhereConditionParseException("Table " + targetTable.getTableName() + 
					" doesn't have a column named '" + this.columnName + "'");
		DataType desiredType = column.getType();
		
		// Casts the values
		castValuesToDataType(desiredType);
		
		// Parses the sql
		return this.columnName + " BETWEEN ? AND ?";
	}
}
