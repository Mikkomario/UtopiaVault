package utopia.vault.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
	protected String toSql() throws ConditionParseException
	{
		return this.column.getColumnName() + " IS NULL";
	}

	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex)
			throws SQLException, ConditionParseException
	{
		// No object values are used in this condition
		return startIndex;
	}

	@Override
	public String getDebugSql()
	{
		return this.column.getColumnName() + " IS NULL";
	}
}
