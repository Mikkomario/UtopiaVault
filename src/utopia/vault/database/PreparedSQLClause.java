package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.Try;

/**
 * This interface is implemented by various clauses that can be used in prepared sql 
 * statements
 * @author Mikko Hilpinen
 * @since 6.7.2016
 */
public interface PreparedSQLClause
{
	// ATTRIBUTES	----------------------------
	
	/**
	 * A regex used as a placeholder for inserted values
	 */
	public static final String VALUE_PLACEHOLDER = "?";
	
	
	// ABSTRACT	--------------------------------
	
	/**
	 * Parses the clause into an sql statement with '?' characters as future value placeholders. 
	 * An example sql could be: 'table1.column1=? AND table1.column2=?'
	 * @return An sql clause
	 * @throws StatementParseException If the clause couldn't be parsed
	 */
	public String toSql() throws StatementParseException;
	
	/**
	 * @return The values inserted to the prepared statement. The size of the list must be 
	 * equal to the amount of '?' place holders in the return value of {@link #toSql()}
	 */
	public ImmutableList<Value> getValues();
	
	
	// OTHER	----------------------------
	
	/**
	 * @return A descriptive string to this sql. Contains values by default.
	 */
	public default String describe()
	{
		return Try.run(this::toSql).handleMap(sql -> 
		{
			String next = sql;
			
			for (Value value : getValues())
			{
				next = next.replaceFirst("\\" + VALUE_PLACEHOLDER, value.toStringOption().getOrElse("NULL"));
			}
			
			return next;
		}, e -> e.getMessage());
	}
}
