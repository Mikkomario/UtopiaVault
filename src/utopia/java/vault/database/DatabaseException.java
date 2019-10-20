package utopia.java.vault.database;

import java.util.HashMap;
import java.util.Map;

import utopia.java.flow.structure.Option;
import utopia.java.vault.util.DebuggableException;
import utopia.java.vault.generics.Column;
import utopia.java.vault.generics.Table;
import utopia.java.vault.generics.VariableNameMapping;
import utopia.java.vault.generics.VariableNameMapping.MappingException;
import utopia.java.vault.generics.VariableNameMapping.NoColumnForVariableException;
import utopia.java.vault.generics.VariableNameMapping.NoVariableForColumnException;

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
	public DatabaseException(StatementParseException cause)
	{
		super(createMessage(cause), cause.getMessage(), cause);
	}
	
	/**
	 * Creates a new exception from a where parse exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param condition The condition that was used
	 */
	public DatabaseException(StatementParseException cause, Condition condition)
	{
		super(createMessage(cause), condition == null ? "no condition" : condition.describe(), cause);
	}
	
	/**
	 * Creates a new exception from a mapping exception.
	 * @param cause The exception that caused the operation to fail
	 * @param mapping The mapping that was being used
	 */
	public DatabaseException(MappingException cause, VariableNameMapping mapping)
	{
		super(createMessage(cause), mapping.getDebugString(), cause);
	}
	
	/**
	 * Creates a new exception from an column mapping exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param table The table that was being used (optional)
	 */
	public DatabaseException(NoVariableForColumnException cause, Table table)
	{
		super(createMessage(cause), parseNoVariableForColumnDebugMessage(cause, table), cause);
	}
	
	/**
	 * Creates a new exception from attribute parse exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param table The table that was being used (optional)
	 */
	public DatabaseException(NoColumnForVariableException cause, Table table)
	{
		super(createMessage(cause), parseNoColumnForVariableDebugMessage(cause, table), cause);
	}
	
	/**
	 * Creates a new exception
	 * @param cause The cause of this exception
	 * @param sqlStatement The sql statement that was being used
	 * @param usedTable The table that was being used
	 * @param whereClause The where condition that was being used (optional)
	 * @param assignedValues The values that were being assigned
	 * @param selection The selected columns
	 */
	public DatabaseException(Throwable cause, String sqlStatement, Table usedTable, 
			Option<Condition> whereClause, ValueAssignment assignedValues, Selection selection)
	{
		super(createMessage(cause), parseDebugMessage(sqlStatement, 
				usedTable, whereClause, assignedValues, selection), cause);
	}
	
	/**
	 * Creates a new exception to be used in incomplete inserts
	 * @param missingColumn The column that was missing from the insert
	 * @param insert The inserted values
	 */
	public DatabaseException(Column missingColumn, ValueAssignment insert)
	{
		super("Column " + missingColumn + " is missing from an insert", 
				parseDebugMessage(null, missingColumn.getTable(), null, insert, null));
	}
	
	/**
	 * Creates a new exception for incomplete insert
	 * @param into The target table
	 * @param insert The inserted values
	 */
	public DatabaseException(Table into, ValueAssignment insert)
	{
		super("Incomplete insert", parseDebugMessage(null, into, null, insert, null));
	}
	
	/**
	 * Creates a new database exception
	 * @param debugmessage The debug message sent with the exception
	 * @param cause The cause of the exception
	 */
	public DatabaseException(String debugmessage, Throwable cause)
	{
		super(createMessage(cause), debugmessage, cause);
	}
	
	/**
	 * Creates a new database exception
	 * @param message The message sent along with the exception
	 * @param sqlStatement The sql statement that was being used
	 * @param usedTable The table that was being used
	 * @param whereClause The where clause used
	 * @param assignedValues The values that were being assigned
	 * @param selection The selection that was being made
	 */
	public DatabaseException(String message, String sqlStatement, Table usedTable, 
			Option<Condition> whereClause, ValueAssignment assignedValues, Selection selection)
	{
		super(message, parseDebugMessage(sqlStatement, usedTable, whereClause, assignedValues, 
				selection));
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
	
	private static String parseNoVariableForColumnDebugMessage(
			NoVariableForColumnException cause, Table table)
	{
		StringBuilder s = new StringBuilder(cause.getMessage());
		s.append("\nCasting ");
		s.append(cause.getColumnName());
		if (table != null)
		{
			s.append(" at ");
			s.append(table);
			
			Option<Column> column = table.findColumnWithColumnName(cause.getColumnName());
			if (column.isDefined())
			{
				s.append("\nColumn: ");
				s.append(column.get());
			}
			
			s.append("\nUsing mapping:\n");
			s.append(table.getNameMapping().getDebugString());
		}
		
		return s.toString();
	}
	
	private static String parseNoColumnForVariableDebugMessage(
			NoColumnForVariableException cause, Table table)
	{
		StringBuilder s = new StringBuilder(cause.getMessage());
		s.append("\nCasting ");
		s.append(cause.getVariableName());
		if (table != null)
		{
			s.append(" to column at ");
			s.append(table);
			
			VariableNameMapping mapper = table.getNameMapping();
			s.append("\nUsing mapping:\n");
			s.append(mapper.getDebugString());
			
			try
			{
				Map<String, String> mappings = new HashMap<>();
				for (Column column : table.getColumns())
				{
					mappings.put(column.getColumnName(), 
							mapper.getVariableName(column.getColumnName()));
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
			catch (NoVariableForColumnException e)
			{
				// Ignored. If mapping fails, it can't be written
			}
		}
		
		return s.toString();
	}
	
	private static String parseDebugMessage(String sqlStatement, Table table, Option<Condition> where, 
			ValueAssignment providedValues, Selection selection)
	{
		StringBuilder message = new StringBuilder();
		if (sqlStatement != null)
			message.append("SQL: " + sqlStatement);
		if (where != null && table != null)
			where.forEach(w -> message.append("\nWhere: " + w.describe()));
		if (table != null)
		{
			message.append("\nTable used: ");
			message.append(table.getDatabaseName() + "/");
			message.append(table.getName());
		}
		if (providedValues != null && !providedValues.isEmpty())
		{
			message.append("\nValues provided: ");
			message.append(providedValues.getDebugDescription());
		}
		if (selection != null)
		{
			message.append("\nSelection: ");
			message.append(selection);
		}
		
		return message.toString();
	}
	
	/*
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
	*/
}
