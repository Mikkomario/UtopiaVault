package utopia.vault.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Conditions can be used in numerous database methods to limit the number of operated 
 * rows, joined rows, etc. in sql queries
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public abstract class Condition
{
	// ABSTRACT METHODS	-------------------
	
	/**
	 * Creates an sql statement based on this where condition. The value spots will be marked 
	 * with '?' for future preparation. This sql statement doesn't include the first "WHERE", 
	 * only the condition (Eg. "columnName <=> ? AND another > ?)"
	 * @return A logical sql condition like 'NOT (columName = ?)'
	 * @throws ConditionParseException If the where condition can't be parsed into sql
	 */
	protected abstract String toSql() throws ConditionParseException;
	
	/**
	 * Sets condition values to the prepared sql statement
	 * @param statement The statement that is being prepared.
	 * @param startIndex The index where the first value of this condition occurs.
	 * @return The index of the next value insert
	 * @throws SQLException If the operation failed due to an sql error
	 * @throws ConditionParseException If the where condition couldn't be parsed 
	 * to a desirable state
	 */
	public abstract int setObjectValues(PreparedStatement statement, int startIndex) throws 
			SQLException, ConditionParseException;
	
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
	public CombinedCondition and(Condition other, Condition... others)
	{
		return CombinedCondition.createANDCombination(this, other, others);
	}
	
	/**
	 * Creates a new where condition from the two (or more) conditions. Only one of the 
	 * conditions has to be true in order for the combined condition to be true
	 * @param other The other condition
	 * @param others The other conditions combined with these two
	 * @return A combination of the conditions
	 */
	public CombinedCondition or(Condition other, Condition... others)
	{
		return CombinedCondition.createORConbination(this, other, others);
	}
	
	/**
	 * Creates a new where condition from the two conditions. The two conditions must have 
	 * different logical values for the combination to be true
	 * @param other The other condition
	 * @return A combination of the conditions
	 */
	public CombinedCondition xor(Condition other)
	{
		return CombinedCondition.createXORCombination(this, other);
	}
	
	/**
	 * Writes the condition as a where clause that includes the " WHERE" statement (including 
	 * the first whitespace).
	 * @return The where condition as a where clause
	 * @throws ConditionParseException If the where condition parsing failed
	 */
	public String toWhereClause() throws ConditionParseException
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
	public static class ConditionParseException extends Exception
	{
		private static final long serialVersionUID = -7800912556657335734L;
		
		// CONSTRUCTOR	--------------------

		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 * @param source The source of the exception
		 */
		public ConditionParseException(String message, Throwable source)
		{
			super(message, source);
		}
		
		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 */
		public ConditionParseException(String message)
		{
			super(message);
		}
	}
}
