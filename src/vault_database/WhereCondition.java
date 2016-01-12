package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import vault_generics.Table;

/**
 * Where conditions can be used in numerous database methods to limit the number of operated 
 * rows
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public abstract class WhereCondition
{
	// ABSTRACT METHODS	-------------------
	
	/**
	 * Creates an sql statement based on this where condition. The value spots will be marked 
	 * with '?' for future preparation. This sql statement doesn't include the first "WHERE", 
	 * only the condition (Eg. "columnName <=> ? AND another > ?)"
	 * @return A logical sql condition like 'NOT (columName = ?)'
	 * @throws WhereConditionParseException If the where condition can't be parsed into sql
	 */
	protected abstract String toSql() throws WhereConditionParseException;
	
	/**
	 * Sets condition values to the prepared sql statement
	 * @param statement The statement that is being prepared.
	 * @param startIndex The index where the first value of this condition occurs.
	 * @return The index of the next value insert
	 * @throws SQLException If the operation failed due to an sql error
	 * @throws WhereConditionParseException If the where condition couldn't be parsed 
	 * to a desirable state
	 */
	public abstract int setObjectValues(PreparedStatement statement, int startIndex) throws 
			SQLException, WhereConditionParseException;
	
	/**
	 * Returns a debug string that mimics the final sql statement created from this condition
	 * @return A debug sql statement
	 */
	public abstract String getDebugSql();
	
	
	// OTHER METHODS	--------------------
	
	/**
	 * Creates a new where condition from the two (or more) conditions. All of the conditions 
	 * must be true for the combined condition to be true.
	 * @param other The other condition
	 * @param others The other conditions combined with these two
	 * @return A combination of the conditions
	 */
	public CombinedWhereCondition and(WhereCondition other, WhereCondition... others)
	{
		return CombinedWhereCondition.createANDCombination(this, other, others);
	}
	
	/**
	 * Creates a new where condition from the two (or more) conditions. Only one of the 
	 * conditions has to be true in order for the combined condition to be true
	 * @param other The other condition
	 * @param others The other conditions combined with these two
	 * @return A combination of the conditions
	 */
	public CombinedWhereCondition or(WhereCondition other, WhereCondition... others)
	{
		return CombinedWhereCondition.createORConbination(this, other, others);
	}
	
	/**
	 * Creates a new where condition from the two conditions. The two conditions must have 
	 * different logical values for the combination to be true
	 * @param other The other condition
	 * @return A combination of the conditions
	 */
	public CombinedWhereCondition xor(WhereCondition other)
	{
		return CombinedWhereCondition.createXORCombination(this, other);
	}
	
	/**
	 * Writes the condition as a where clause that includes the " WHERE" statement (including 
	 * the first whitespace).
	 * @param targetTable The table the condition is used in
	 * @return The where condition as a where clause
	 * @throws WhereConditionParseException If the where condition parsing failed
	 */
	public String toWhereClause(Table targetTable) throws WhereConditionParseException
	{
		String sql = toSql();
		
		if (sql.isEmpty())
			return sql;
		else
			return " WHERE " + sql;
	}
	
	
	// SUBCLASSES	------------------------
	
	/**
	 * These exceptions are thrown when conditions can't be parsed / used
	 * @author Mikko Hilpinen
	 * @since 2.10.2015
	 */
	public static class WhereConditionParseException extends Exception
	{
		private static final long serialVersionUID = -7800912556657335734L;
		
		// CONSTRUCTOR	--------------------

		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 * @param source The source of the exception
		 */
		public WhereConditionParseException(String message, Throwable source)
		{
			super(message, source);
		}
		
		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 */
		public WhereConditionParseException(String message)
		{
			super(message);
		}
	}
}
