package vault_database;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import vault_database.AttributeNameMapping;
import vault_database.AttributeNameMapping.MappingException;
import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DatabaseTable;
import vault_database.DatabaseTable.Column;
import vault_database.WhereCondition;
import vault_database.WhereCondition.WhereConditionParseException;
import vault_recording.DatabaseWritable;
import vault_util.DebuggableException;

/**
 * This exception wraps multiple exceptions that are caused by logic errors in database use.
 * @author Mikko Hilpinen
 * @since 20.10.2015
 */
public class DatabaseException extends DebuggableException
{
	// ATTRIBUTES	-------------------
	
	private static final long serialVersionUID = -5853057676787454794L;
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new exception with no special debug message
	 * @param cause The exception that caused the operation to fail
	 */
	public DatabaseException(Throwable cause)
	{
		super(createMessage(cause), cause.getMessage() == null ? "" : cause.getMessage(), 
				cause);
	}
	
	/**
	 * Creates a new exception from another debuggable exception. The debug message is copied.
	 * @param cause The exception that caused the operation to fail
	 */
	public DatabaseException(DebuggableException cause)
	{
		super(createMessage(cause), cause.getDebugMessage(), cause);
	}
	
	/**
	 * Creates a new exception from a where parse exception, using no special debug message
	 * @param cause The exception that caused the operation to fail
	 */
	public DatabaseException(WhereConditionParseException cause)
	{
		super(createMessage(cause), cause.getMessage(), cause);
	}
	
	/**
	 * Creates a new exception from a where parse exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param table The table that was being used (optional)
	 * @param where The where condition that was being used (optional)
	 */
	public DatabaseException(WhereConditionParseException cause, 
			DatabaseTable table, WhereCondition where)
	{
		super(createMessage(cause), parseWhereConditionDebugMessage(cause, where, table), cause);
	}
	
	/**
	 * Creates a new exception from a mapping exception.
	 * @param cause The exception that caused the operation to fail
	 * @param mapping The mapping that was being used
	 */
	public DatabaseException(MappingException cause, AttributeNameMapping mapping)
	{
		super(createMessage(cause), mapping.getDebugString(), cause);
	}
	
	/**
	 * Creates a new exception from an column mapping exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param table The table that was being used (optional)
	 */
	public DatabaseException(NoAttributeForColumnException cause, DatabaseTable table)
	{
		super(createMessage(cause), parseNoAttributeForColumnDebugMessage(cause, table), cause);
	}
	
	/**
	 * Creates a new exception from attribute parse exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param table The table that was being used (optional)
	 */
	public DatabaseException(NoColumnForAttributeException cause, DatabaseTable table)
	{
		super(createMessage(cause), parseNoColumnForAttributeDebugMessage(cause, table), cause);
	}
	
	/**
	 * Creates a new exception
	 * @param cause The cause of this exception
	 * @param sqlStatement The sql statement that was being used
	 * @param usedTable The table that was being used
	 * @param whereClause The where condition that was being used (optional)
	 * @param providedValues The values that had been provided (optional)
	 */
	public DatabaseException(Throwable cause, String sqlStatement, DatabaseTable usedTable, 
			WhereCondition whereClause, Collection<? extends Attribute> providedValues)
	{
		super(createMessage(cause), parseDebugMessage(sqlStatement, 
				usedTable, whereClause, providedValues), cause);
	}
	
	/**
	 * Creates a new exception from an indexAttributeRequiredException. Full debug message parsed.
	 * @param cause The cause of the exception
	 * @param model The model that was missing an index attribute
	 */
	public DatabaseException(IndexAttributeRequiredException cause, DatabaseWritable model)
	{
		super(createMessage(cause), parseIndexDebugMessage(model), cause);
	}

	
	// OTHER METHODS	---------------
	
	private static String createMessage(Throwable cause)
	{
		String className = cause.getClass().getSimpleName();
		if (className == null)
			className = "?";
		
		String message = cause.getMessage();
		if (message == null)
			message = "";
		
		return className + ": '" + message + "'";
	}
	
	private static String parseWhereConditionDebugMessage(Throwable cause, WhereCondition where, 
			DatabaseTable table)
	{
		StringBuilder s = new StringBuilder(cause.getMessage());
		if (table != null)
		{
			if (where != null && table != null)
			{
				s.append("\nWhere: ");
				s.append(where.getDebugSql(table));
			}
			
			s.append("\nWith table: ");
			s.append(table);
		}
		
		return s.toString();
	}
	
	private static String parseNoAttributeForColumnDebugMessage(
			NoAttributeForColumnException cause, DatabaseTable table)
	{
		StringBuilder s = new StringBuilder(cause.getMessage());
		s.append("\nCasting ");
		s.append(cause.getColumnName());
		if (table != null)
		{
			s.append(" at ");
			s.append(table);
			
			Column column = DatabaseTable.findColumnWithName(table.getColumnInfo(), 
					cause.getColumnName());
			if (column != null)
			{
				s.append("\nColumn: ");
				s.append(column);
			}
			
			s.append("\nUsing mapping:\n");
			s.append(table.getAttributeNameMapping().getDebugString());
		}
		
		return s.toString();
	}
	
	private static String parseNoColumnForAttributeDebugMessage(
			NoColumnForAttributeException cause, DatabaseTable table)
	{
		StringBuilder s = new StringBuilder(cause.getMessage());
		s.append("\nCasting ");
		s.append(cause.getAttributeName());
		if (table != null)
		{
			s.append(" to column at ");
			s.append(table);
			
			AttributeNameMapping mapper = table.getAttributeNameMapping();
			s.append("\nUsing mapping:\n");
			s.append(mapper.getDebugString());
			
			try
			{
				Map<String, String> mappings = new HashMap<>();
				for (Column column : table.getColumnInfo())
				{
					mappings.put(column.getName(), mapper.getAttributeName(column.getName()));
				}
				
				// Writes all correct mappings as well
				s.append("\nCorrect mappings:");
				boolean isFirst = true;
				for (String columnName : mappings.keySet())
				{
					if (!isFirst)
						s.append(", ");
					else
						isFirst = false;
					
					s.append(columnName);
					s.append(" <=> ");
					s.append(mappings.get(columnName));
				}
			}
			catch (NoAttributeForColumnException e)
			{
				// Ignored. If mapping fails, it can't be written
			}
		}
		
		return s.toString();
	}
	
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
			message.append("\nAttributes provided:");
			for (Attribute attribute : providedValues)
			{
				message.append("\n");
				message.append(attribute);
			}
		}
		
		return message.toString();
	}
	
	private static String parseIndexDebugMessage(DatabaseWritable model)
	{
		StringBuilder s = new StringBuilder("Model: ");
		s.append(model);
		
		s.append(" from table ");
		s.append(model.getTable());
		
		s.append("\nWith attributes:");
		for (Attribute attribute : model.getAttributes())
		{
			s.append("\n");
			s.append(attribute);
		}
		
		Column indexColumn = model.getTable().getPrimaryColumn();
		if (indexColumn == null)
			s.append("\nTable doesn't have a primary column");
		else
		{
			s.append("\nIndex column: ");
			s.append(indexColumn);
			
			try
			{
				String indexAttName = model.getTable().getAttributeNameMapping().getAttributeName(
						indexColumn.getName());
				s.append("\nRequired attribute: ");
				s.append(indexAttName);
			}
			catch (NoAttributeForColumnException e)
			{
				s.append("\nCan't parse index attribute name");
			}
		}
		
		return s.toString();
	}
}
