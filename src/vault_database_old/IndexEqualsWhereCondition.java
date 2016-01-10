package vault_database_old;

import vault_database_old.DatabaseTable.Column;

/**
 * This where condition checks the value of a primary index attribute
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public class IndexEqualsWhereCondition extends SingleWhereCondition
{
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition that is true only for the provided index value
	 * @param inverted Should the condition be inverted (true for every row except the one 
	 * with the provided index value)
	 * @param indexValue The value of the index the row must have
	 */
	public IndexEqualsWhereCondition(boolean inverted, DatabaseValue indexValue)
	{
		super(inverted, indexValue);
	}
	
	
	// IMPLEMENTED METHODS	----------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable)
			throws WhereConditionParseException
	{
		// Updates the data type
		Column indexColumn = targetTable.getPrimaryColumn();
		if (indexColumn == null)
			throw new WhereConditionParseException(
					"Can't use index where condition for table (" + targetTable.getTableName() 
					+ ") that doesn't have a primary column");
		
		// Parses the index to the correct type
		castValuesToDataType(indexColumn.getType());
		
		// Creates the sql
		return indexColumn.getName() + " <=> ?";
	}


	@Override
	protected String getDebugSqlWithNoParsing(DatabaseTable targetTable)
	{
		Column indexColumn = targetTable.getPrimaryColumn();
		
		if (indexColumn == null)
			return "index (missing) <=> ?";
		else
			return indexColumn.getName() + " <=> ?";
	}
}
