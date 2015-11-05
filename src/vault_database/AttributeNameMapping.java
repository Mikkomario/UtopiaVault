package vault_database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import vault_database.DatabaseTable.Column;

/**
 * This map connects column names to attribute names. All names are case-insensitive. 
 * "STRING" would be considered equal with "string"
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class AttributeNameMapping
{
	// ATTRIBUTES	-------------------
	
	private Map<String, String> names, columnCasing; // column name, attribute name | 
	// columnName (lower), columnName (correct)
	private List<NameMappingRule> rules;
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new empty mapping. Attribute names must be added separately.
	 */
	public AttributeNameMapping()
	{
		this.names = new HashMap<>();
		this.rules = new ArrayList<>();
		this.columnCasing = new HashMap<>();
	}

	
	// OTHER METHODS	---------------
	
	/**
	 * Finds the attribute name mapped to the column name. Creates a new mapping based on an 
	 * existing rule if previous mapping doesn't exists and possible.
	 * @param columnName The column name
	 * @return An attribute name mapped to the column name.
	 * @throws NoAttributeForColumnException If the attribute name couldn't be found
	 */
	public String getAttributeName(String columnName) throws NoAttributeForColumnException
	{	
		// If there's already a mapping for the column, uses that
		if (containsMappingForColumn(columnName))
			return this.names.get(columnName.toLowerCase());
		
		// Otherwise checks if a rule can be applied
		for (NameMappingRule rule : this.rules)
		{
			if (rule.canMapColumnName(columnName))
			{
				String attributeName = rule.getAttributeName(columnName).toLowerCase();
				addMapping(columnName, attributeName);
				return attributeName;
			}
		}
		
		throw new NoAttributeForColumnException(columnName);
	}
	
	/**
	 * Finds a column name mapped to this attribute name. If there is no direct mapping, tries 
	 * to use rules to define the column name. If a rule is applied, creates a new mapping.
	 * @param attributeName The attribute name
	 * @return A column name mapped to the provided attribute name
	 * @throws NoColumnForAttributeException If the column name couldn't be found
	 */
	public String getColumnName(String attributeName) throws NoColumnForAttributeException
	{
		// Searches for a direct mapping
		for (String columnName : this.names.keySet())
		{
			if (this.names.get(columnName).equalsIgnoreCase(attributeName))
				return getCorrectCasingForColumnName(columnName);
		}
		
		// Tries to use applied rules
		for (NameMappingRule rule : this.rules)
		{
			if (rule.canRetraceColumnName(attributeName))
			{
				String columnName = rule.getColumnName(attributeName);
				addMapping(columnName, attributeName);
				return columnName;
			}
		}
		
		throw new NoColumnForAttributeException(attributeName);
	}
	
	/**
	 * Creates a new name association, possible previous association will be replaced
	 * @param columnName The name of the column
	 * @param attributeName The attribute name mapped to the column name
	 */
	public void addMapping(String columnName, String attributeName)
	{
		this.names.put(columnName.toLowerCase(), attributeName.toLowerCase());
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
	 * Checks if any column is mapped to the provided attribute name
	 * @param attributeName The attribute name some columns may be mapped to
	 * @return Is there a column name mapped to the provided attribute name
	 */
	public boolean containsMappingForAttribute(String attributeName)
	{
		return this.names.keySet().contains(attributeName.toLowerCase());
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
					getAttributeName(column.getName());
				}
				catch (NoAttributeForColumnException e)
				{
					// Ignored
				}
			}
		}
	}
	
	/**
	 * Searches through the collection for a column that is mapped to the given attribute name
	 * @param columns The columns
	 * @param attributeName The name of the attribute that represents a column
	 * @return The column the mapped to the attribute name or null if no such column exists in 
	 * the collection
	 * @throws NoAttributeForColumnException If the operation possibly failed because a 
	 * column name couldn't be mapped to an attribute name
	 */
	public Column findColumnForAttribute(Collection<? extends Column> columns, 
			String attributeName) throws NoAttributeForColumnException
	{
		NoAttributeForColumnException latestException = null;
		
		for (Column column : columns)
		{
			try
			{
				if (getAttributeName(column.getName()).equalsIgnoreCase(attributeName))
					return column;
			}
			catch (NoAttributeForColumnException e)
			{
				latestException = e;
			}
		}
		
		if (latestException != null)
			throw latestException;
		
		return null;
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
					s.append(getAttributeName(columnName));
				}
				catch (NoAttributeForColumnException e)
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
	 * These exceptions are thrown when a mapping fails to get an attribute name from a 
	 * provided column name
	 * @author Mikko Hilpinen
	 * @since 24.9.2015
	 */
	public static class NoAttributeForColumnException extends MappingException
	{
		// ATTRIBUTES	---------------------
		
		private static final long serialVersionUID = 3016147572881469183L;
		private String columnName;
		
		
		// CONSTRUCTOR	---------------------
		
		/**
		 * Creates a new exception
		 * @param columnName The column name that couldn't be mapped to an attribute name
		 */
		public NoAttributeForColumnException(String columnName)
		{
			super("Can't find attribute name for column name '" + columnName + "'");
			this.columnName = columnName;
		}
		
		
		// ACCESSORS	---------------------
		
		/**
		 * @return The column name that couldn't be mapped to an attribute name
		 */
		public String getColumnName()
		{
			return this.columnName;
		}
	}
	
	/**
	 * These exceptions are thrown when a mapping fails to retrace a column name from an 
	 * attribute name
	 * @author Mikko Hilpinen
	 * @since 24.9.2015
	 */
	public static class NoColumnForAttributeException extends MappingException
	{
		// ATTRIBUTES	---------------------
		
		private static final long serialVersionUID = 3016147572881469183L;
		private String attributeName;
		
		
		// CONSTRUCTOR	---------------------
		
		/**
		 * Creates a new exception
		 * @param attributeName The attribute name that couldn't be retraced to a column name
		 */
		public NoColumnForAttributeException(String attributeName)
		{
			super("Can't find column name for attribute name '" + attributeName + "'");
			this.attributeName = attributeName;
		}
		
		
		// ACCESSORS	---------------------
		
		/**
		 * @return The column name that couldn't be mapped to an attribute name
		 */
		public String getAttributeName()
		{
			return this.attributeName;
		}
	}
}
