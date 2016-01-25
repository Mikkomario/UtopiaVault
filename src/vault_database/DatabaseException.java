package vault_database;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import utopia.flow.generics.Variable;
import utopia.flow.generics.VariableDeclaration;
import vault_database.Condition.ConditionParseException;
import vault_generics.Column;
import vault_generics.Table;
import vault_generics.VariableNameMapping;
import vault_generics.VariableNameMapping.MappingException;
import vault_generics.VariableNameMapping.NoColumnForVariableException;
import vault_generics.VariableNameMapping.NoVariableForColumnException;
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
	public DatabaseException(ConditionParseException cause)
	{
		super(createMessage(cause), cause.getMessage(), cause);
	}
	
	/**
	 * Creates a new exception from a where parse exception. Full debug message parsed.
	 * @param cause The exception that caused the operation to fail
	 * @param condition The condition that was used
	 */
	public DatabaseException(ConditionParseException cause, Condition condition)
	{
		super(createMessage(cause), condition == null ? "no condition" : condition.getDebugSql(), 
				cause);
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
	 * @param providedValues The values that had been provided (optional)
	 * @param selection The selected columns
	 */
	public DatabaseException(Throwable cause, String sqlStatement, Table usedTable, 
			Condition whereClause, Collection<? extends Variable> providedValues, 
			Collection<? extends VariableDeclaration> selection)
	{
		super(createMessage(cause), parseDebugMessage(sqlStatement, 
				usedTable, whereClause, providedValues, selection), cause);
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
			
			Column column = table.findColumnWithColumnName(cause.getColumnName());
			if (column != null)
			{
				s.append("\nColumn: ");
				s.append(column);
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
	
	private static String parseDebugMessage(String sqlStatement, Table table, 
			Condition where, Collection<? extends Variable> providedValues, 
			Collection<? extends VariableDeclaration> selection)
	{
		StringBuilder message = new StringBuilder();
		if (sqlStatement != null)
			message.append("SQL: " + sqlStatement);
		if (where != null && table != null)
			message.append("\nWhere: " + where.getDebugSql());
		if (table != null)
		{
			message.append("\nTable used: ");
			message.append(table.getDatabaseName() + "/");
			message.append(table.getName());
		}
		if (providedValues != null && !providedValues.isEmpty())
		{
			message.append("\nValues provided:");
			for (Variable var : providedValues)
			{
				message.append("\n");
				message.append(var);
			}
		}
		if (selection != null && !selection.isEmpty())
		{
			message.append("\nSelection:");
			for (VariableDeclaration dec : selection)
			{
				message.append("\n");
				message.append(dec);
			}
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
