package utopia.java.vault.generics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utopia.java.flow.structure.ImmutableList;

/**
 * This map connects column names to variable names. All names are case-insensitive. 
 * "STRING" would be considered equal with "string"
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class VariableNameMapping
{
	// variableS	-------------------
	
	private Map<String, String> names, columnCasing; // column name, variable name | 
	// columnName (lower), columnName (correct)
	private List<NameMappingRule> rules;
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new empty mapping. Variable names must be added separately.
	 */
	public VariableNameMapping()
	{
		this.names = new HashMap<>();
		this.rules = new ArrayList<>();
		this.columnCasing = new HashMap<>();
	}

	
	// OTHER METHODS	---------------
	
	/**
	 * Finds the variable name mapped to the column name. Creates a new mapping based on an 
	 * existing rule if previous mapping doesn't exists and possible.
	 * @param columnName The column name
	 * @return An variable name mapped to the column name.
	 * @throws NoVariableForColumnException If the variable name couldn't be found
	 */
	public String getVariableName(String columnName) throws NoVariableForColumnException
	{	
		// If there's already a mapping for the column, uses that
		if (containsMappingForColumn(columnName))
			return this.names.get(columnName.toLowerCase());
		
		// Otherwise checks if a rule can be applied
		for (NameMappingRule rule : this.rules)
		{
			if (rule.canMapColumnName(columnName))
			{
				String variableName = rule.getVariableName(columnName).toLowerCase();
				addMapping(columnName, variableName);
				return variableName;
			}
		}
		
		throw new NoVariableForColumnException(columnName);
	}
	
	/**
	 * Finds a column name mapped to this variable name. If there is no direct mapping, tries 
	 * to use rules to define the column name. If a rule is applied, creates a new mapping.
	 * @param variableName The variable name
	 * @return A column name mapped to the provided variable name
	 * @throws NoColumnForVariableException If the column name couldn't be found
	 */
	public String getColumnName(String variableName) throws NoColumnForVariableException
	{
		// Searches for a direct mapping
		for (String columnName : this.names.keySet())
		{
			if (this.names.get(columnName).equalsIgnoreCase(variableName))
				return getCorrectCasingForColumnName(columnName);
		}
		
		// Tries to use applied rules
		for (NameMappingRule rule : this.rules)
		{
			if (rule.canRetraceColumnName(variableName))
			{
				String columnName = rule.getColumnName(variableName);
				addMapping(columnName, variableName);
				return columnName;
			}
		}
		
		throw new NoColumnForVariableException(variableName);
	}
	
	/**
	 * Creates a new name association, possible previous association will be replaced
	 * @param columnName The name of the column
	 * @param variableName The variable name mapped to the column name
	 */
	public void addMapping(String columnName, String variableName)
	{
		this.names.put(columnName.toLowerCase(), variableName.toLowerCase());
		this.columnCasing.put(columnName.toLowerCase(), columnName);
	}
	
	/**
	 * Checks if there is a mapping for the provided column name
	 * @param columnName The column name
	 * @return Is there a mapping for the provided column name
	 */
	public boolean containsMappingForColumn(String columnName)
	{
		return this.names.containsKey(columnName.toLowerCase());
	}
	
	/**
	 * Checks if any column is mapped to the provided variable name
	 * @param variableName The variable name some columns may be mapped to
	 * @return Is there a column name mapped to the provided variable name
	 */
	public boolean containsMappingForVariable(String variableName)
	{
		return this.names.keySet().contains(variableName.toLowerCase());
	}
	
	/**
	 * Adds a new rule that will be applied when there is no direct mapping. Multiple rules 
	 * can be applied, later being applied when the former can't be used.
	 * @param rule The new name mapping rule
	 */
	public void addRule(NameMappingRule rule)
	{
		if (!this.rules.contains(rule))
			this.rules.add(rule);
	}
	
	/**
	 * This method adds a mapping for each of the columns in the provided collection using the 
	 * rules affecting this mapping. If there is already a mapping or no rule can be applied, 
	 * doesn't create a mapping for the column.
	 * @param columns The columns that should get mapped.
	 */
	public void addMappingForEachColumnWherePossible(Collection<? extends Column> columns)
	{
		if (columns != null)
		{
			for (Column column : columns)
			{
				try
				{
					getVariableName(column.getColumnName());
				}
				catch (NoVariableForColumnException e)
				{
					// Ignored
				}
			}
		}
	}
	
	/**
	 * This method adds a mapping for each of the columns in the provided collection using the 
	 * rules affecting this mapping. If there is already a mapping or no rule can be applied, 
	 * doesn't create a mapping for the column.
	 * @param columns The columns that should get mapped.
	 */
	public void addMappingForEachColumnWherePossible(ImmutableList<? extends Column> columns)
	{
		if (columns != null)
		{
			for (Column column : columns)
			{
				try
				{
					getVariableName(column.getColumnName());
				}
				catch (NoVariableForColumnException e)
				{
					// Ignored
				}
			}
		}
	}
	
	/**
	 * @return Parses a debug message that describes the contents of this mapping
	 */
	public String getDebugString()
	{
		StringBuilder s = new StringBuilder();
		if (this.columnCasing.isEmpty())
			s.append("No existing mappings");
		else
		{
			s.append("Mappings:");
			for (String columnName : this.columnCasing.values())
			{
				s.append("\n");
				s.append(columnName);
				s.append(" <=> ");
				try
				{
					s.append(getVariableName(columnName));
				}
				catch (NoVariableForColumnException e)
				{
					s.append("ERROR");
				}
			}
		}
		
		if (!this.rules.isEmpty())
		{
			s.append("\nRules:");
			for (NameMappingRule rule : this.rules)
			{
				s.append("\n");
				String className = rule.getClass().getSimpleName();
				if (className != null)
					s.append(className);
				else
					s.append("?");
			}
		}
		
		return s.toString();
	}
	
	private String getCorrectCasingForColumnName(String columnName)
	{
		String correct = this.columnCasing.get(columnName);
		if (correct != null)
			return correct;
		return columnName;
	}
	
	
	// SUBCLASSES	-------------------------
	
	/**
	 * Mapping exceptions are thrown when a mapping operation fails
	 * @author Mikko Hilpinen
	 * @since 24.9.2015
	 */
	public static class MappingException extends Exception
	{
		private static final long serialVersionUID = -4642594718868686062L;
		
		// CONSTRUCTOR	---------------------

		/**
		 * Creates a new mapping exception
		 * @param message The message sent along with the exception
		 */
		public MappingException(String message)
		{
			super(message);
		}
	}
	
	/**
	 * These exceptions are thrown when a mapping fails to get an variable name from a 
	 * provided column name
	 * @author Mikko Hilpinen
	 * @since 24.9.2015
	 */
	public static class NoVariableForColumnException extends MappingException
	{
		// variableS	---------------------
		
		private static final long serialVersionUID = 3016147572881469183L;
		private String columnName;
		
		
		// CONSTRUCTOR	---------------------
		
		/**
		 * Creates a new exception
		 * @param columnName The column name that couldn't be mapped to an variable name
		 */
		public NoVariableForColumnException(String columnName)
		{
			super("Can't find variable name for column name '" + columnName + "'");
			this.columnName = columnName;
		}
		
		
		// ACCESSORS	---------------------
		
		/**
		 * @return The column name that couldn't be mapped to an variable name
		 */
		public String getColumnName()
		{
			return this.columnName;
		}
	}
	
	/**
	 * These exceptions are thrown when a mapping fails to retrace a column name from an 
	 * variable name
	 * @author Mikko Hilpinen
	 * @since 24.9.2015
	 */
	public static class NoColumnForVariableException extends MappingException
	{
		// variableS	---------------------
		
		private static final long serialVersionUID = 3016147572881469183L;
		private String variableName;
		
		
		// CONSTRUCTOR	---------------------
		
		/**
		 * Creates a new exception
		 * @param variableName The variable name that couldn't be retraced to a column name
		 */
		public NoColumnForVariableException(String variableName)
		{
			super("Can't find column name for variable name '" + variableName + "'");
			this.variableName = variableName;
		}
		
		
		// ACCESSORS	---------------------
		
		/**
		 * @return The column name that couldn't be mapped to an variable name
		 */
		public String getVariableName()
		{
			return this.variableName;
		}
	}
}
