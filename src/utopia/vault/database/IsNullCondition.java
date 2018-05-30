package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.vault.generics.Column;

/**
 * This condition checks whether a column is null
 * @author Mikko Hilpinen
 * @since 15.3.2016
 */
public class IsNullCondition extends Condition
{
	// ATTRIBUTES	----------------
	
	private Column column;
	
	
	// CONSTRUCTOR	----------------
	
	/**
	 * Creates a new condition that checks whether the provided column has a null value
	 * @param column A column
	 */
	public IsNullCondition(Column column)
	{
		this.column = column;
	}
	
	
	// IMPLEMENTED METHODS	--------

	@Override
	public String toSql() throws StatementParseException
	{
		return this.column.getColumnName() + " IS NULL";
	}
	
	@Override
	public ImmutableList<Value> getValues()
	{
		// Null conditions don't contain values
		return ImmutableList.empty();
	}

	/*
	@Override
	public String getDebugSql()
	{
		return this.column.getColumnName() + " IS NULL";
	}*/
}
