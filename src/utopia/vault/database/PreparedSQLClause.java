package utopia.vault.database;

import utopia.flow.generics.Value;

/**
 * This interface is implemented by various clauses that can be used in prepared sql 
 * statements
 * @author Mikko Hilpinen
 * @since 6.7.2016
 */
public interface PreparedSQLClause
{
	/**
	 * Parses the clause into an sql statement with '?' characters as future value placeholders. 
	 * An example sql could be: 'table1.column1=? AND table1.column2=?'
	 * @return An sql clause
	 * @throws StatementParseException If the clause couldn't be parsed
	 */
	public String toSql() throws StatementParseException;
	
	/**
	 * @return The values inserted to the prepared statement. The length of the array must be 
	 * equal to the amount of '?' place holders in the return value of {@link #toSql()}
	 */
	public Value[] getValues();
}
