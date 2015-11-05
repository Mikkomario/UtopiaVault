package vault_database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_util.DebuggableException;

/**
 * These exceptions are thrown when using the DatabaseAccessor interface. The exception 
 * contains useful information about the event
 * @author Mikko Hilpinen
 * @since 18.9.2015
 * @deprecated Use {@link DatabaseException} instead
 */
public class VaultSQLException extends DebuggableException
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
	public VaultSQLException(Throwable cause, String sqlStatement, DatabaseTable usedTable, 
			WhereCondition whereClause, Collection<? extends Attribute> providedValues)
	{
		super("Error in sql statement: '" + sqlStatement + "'", parseDebugMessage(sqlStatement, 
				usedTable, whereClause, providedValues), cause);
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
	
	private static String parseDebugMessage(String sqlStatement, DatabaseTable table, 
			WhereCondition where, Collection<? extends Attribute> providedValues)
	{
		StringBuilder message = new StringBuilder("Exception message\n");
		if (sqlStatement != null)
			message.append("SQL: " + sqlStatement);
		if (where != null && table != null)
			message.append("\nWhere debug message: " + 
					where.getDebugSql(table));
		if (table != null)
		{
			message.append("\nTable used: ");
			message.append(table.getDatabaseName() + "/");
			message.append(table.getTableName());
		}
		if (providedValues != null && !providedValues.isEmpty())
		{
			message.append("\nAttributes provided:\n");
			for (Attribute attribute : providedValues)
			{
				message.append(attribute);
				message.append("\n");
			}
		}
		
		return message.toString();
	}
}
