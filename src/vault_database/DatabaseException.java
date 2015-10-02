package vault_database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * These exceptions are thrown when using the DatabaseAccessor interface. The exception 
 * contains useful information about the event
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class DatabaseException extends Exception
{
	// ATTRIBUTES	--------------------
	
	private static final long serialVersionUID = -4209370604592754075L;
	private String sqlStatement;
	private List<Attribute> providedValues;
	private DatabaseTable usedTable;
	private WhereCondition where;
	
	
	// CONSTRUCTOR	--------------------

	/**
	 * Creates a new exception
	 * @param cause The (sql) exception that caused this one
	 * @param sqlStatement The sql statement that was prepared before the exception occurred
	 * @param usedTable The table that was used when the exception occurred
	 * @param whereClause The where clause used when the error occurred (optional)
	 * @param providedValues The values that were used when the exception occurred
	 */
	public DatabaseException(Throwable cause, String sqlStatement, DatabaseTable usedTable, 
			WhereCondition whereClause, Collection<? extends Attribute> providedValues)
	{
		super("Error in sql statement: '" + sqlStatement + "'", cause);
		this.providedValues = new ArrayList<>();
		if (providedValues != null)
			this.providedValues.addAll(providedValues);
		this.where = whereClause;
		this.usedTable = usedTable;
		this.sqlStatement = sqlStatement;
	}
	
	
	// ACCESSORS	---------------------
	
	/**
	 * @return The sql statement that was prepared before the exception occurred
	 */
	public String getSQLStatement()
	{
		return this.sqlStatement;
	}
	
	/**
	 * @return The values that were used when the exception occurred
	 */
	public List<Attribute> getProvidedValues()
	{
		return this.providedValues;
	}
	
	/**
	 * @return The table that was used when the exception occurred
	 */
	public DatabaseTable getUsedTable()
	{
		return this.usedTable;
	}
	
	/**
	 * @return The where clause that was used
	 */
	public WhereCondition getUsedWhereCondition()
	{
		return this.where;
	}
	
	
	// OTHER METHODS	------------------
	
	/**
	 * @return The debug message string that contains all data about the exception (except 
	 * for the source exception)
	 */
	public String getDebugMessage()
	{
		StringBuilder message = new StringBuilder("Exception message\n");
		message.append("SQL: " + getSQLStatement());
		if (getUsedWhereCondition() != null)
			message.append("\nWhere debug message: " + 
					getUsedWhereCondition().getDebugSql(getUsedTable()));
		message.append("\nTable used: ");
		message.append(getUsedTable().getDatabaseName() + "/");
		message.append(getUsedTable().getTableName());
		if (getProvidedValues() != null && !getProvidedValues().isEmpty())
		{
			message.append("\nAttributes provided:\n");
			for (Attribute attribute : getProvidedValues())
			{
				message.append(attribute);
				message.append("\n");
			}
		}
		
		return message.toString();
	}
}
