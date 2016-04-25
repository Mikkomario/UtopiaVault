package utopia.vault.generics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A database table represents a table in a database and contains the necessary column 
 * information
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class Table
{
	// ATTRIBUTES	-------------
	
	private String databaseName, name;
	private VariableNameMapping nameMapping;
	private ColumnInitialiser initialiser;
	
	private List<Column> columns = null;
	private Column primaryColumn = null;
	
	// TODO: Add fuzzy logic for column search
	
	
	// CONSTRUCTOR	-------------
	
	/**
	 * Creates a new database table
	 * @param databaseName The name of the database the table uses
	 * @param name The name of the table
	 * @param nameMapping The name mapping the table uses
	 * @param initialiser The initialiser that is able to initialise the table's column data 
	 * when necessary
	 */
	public Table(String databaseName, String name, VariableNameMapping nameMapping, 
			ColumnInitialiser initialiser)
	{
		this.databaseName = databaseName;
		this.name = name;
		this.nameMapping = nameMapping;
		this.initialiser = initialiser;
	}
	
	
	// IMPLEMENTED METHODS	-----
	
	@Override
	public String toString()
	{
		return getName();
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((getDatabaseName() == null) ? 0 : getDatabaseName().hashCode());
		result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Table))
			return false;
		Table other = (Table) obj;
		if (getDatabaseName() == null)
		{
			if (other.getDatabaseName() != null)
				return false;
		}
		else if (!getDatabaseName().equals(other.getDatabaseName()))
			return false;
		if (getName() == null)
		{
			if (other.getName() != null)
				return false;
		}
		else if (!getName().equals(other.name))
			return false;
		return true;
	}
	
	
	// ACCESSORS	-------------

	/**
	 * @return The name of the database the table uses
	 */
	public String getDatabaseName()
	{
		return this.databaseName;
	}
	
	/**
	 * @return The name of the table in the database
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @return The attribute name mapping that defines the variable names for the columns
	 */
	public VariableNameMapping getNameMapping()
	{
		return this.nameMapping;
	}
	
	/**
	 * @return The columns in this table
	 * @throws DatabaseTableInitialisationException If the columns couldn't be initialised, 
	 * for some reason
	 */
	public List<? extends Column> getColumns() throws DatabaseTableInitialisationException
	{
		// Reads the column data from the database when first requested
		if (this.columns == null)
		{
			this.columns = new ArrayList<>(this.initialiser.generateColumns(this));
			// Also initialises the mappings
			getNameMapping().addMappingForEachColumnWherePossible(this.columns);
		}
		
		return this.columns;
	}
	
	/**
	 * Finds a set of columns that are represented by the provided variable names
	 * @param variableNames The names of the variables whose columns are requested
	 * @return A list of columns containing a column for each of the provided variable names 
	 * that has a corresponding column in this table
	 */
	public List<Column> getVariableColumns(Collection<String> variableNames)
	{
		List<Column> columns = new ArrayList<>();
		for (Column column : getColumns())
		{
			for (String variableName : variableNames)
			{
				if (variableName.equalsIgnoreCase(column.getName()))
				{
					columns.add(column);
					break;
				}
			}
		}
		
		return columns;
	}
	
	/**
	 * Finds a set of columns that are represented by the provided variable names
	 * @param variableNames The names of the variables whose columns are requested
	 * @return A list of columns containing a column for each of the provided variable names 
	 * that has a corresponding column in this table
	 */
	public List<Column> getVariableColumns(String... variableNames)
	{
		List<String> varNames = new ArrayList<>();
		for (String varName : variableNames)
		{
			varNames.add(varName);
		}
		
		return getVariableColumns(varNames);
	}
	
	/**
	 * @return The primary column in this table.
	 * @throws NoSuchColumnException If there is no primary column in this table
	 */
	public Column getPrimaryColumn() throws NoSuchColumnException
	{
		Column primary = findPrimaryColumn();
		if (primary == null)
			throw new NoSuchColumnException("Table " + this + " doesn't have a primary column");
		else
			return primary;
	}
	
	/**
	 * @return The primary column in this table. Null if there is no primary column
	 */
	public Column findPrimaryColumn()
	{
		if (this.primaryColumn == null)
		{
			for (Column column : getColumns())
			{
				if (column.isPrimary())
				{
					this.primaryColumn = column;
					break;
				}
			}
		}
		
		return this.primaryColumn;
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * @return Does the table's primary key use auto increment indexing
	 */
	public boolean usesAutoIncrementIndexing()
	{
		Column primary = findPrimaryColumn();
		if (primary == null)
			return false;
		else
			return primary.usesAutoIncrementIndexing();
	}
	
	/**
	 * @return A list containing the name of each column in the table
	 */
	public List<String> getColumnNames()
	{
		List<String> names = new ArrayList<>();
		for (Column column : getColumns())
		{
			names.add(column.getColumnName());
		}
		
		return names;
	}
	
	/**
	 * @return A list containing the variable names of the columns in the table
	 */
	public List<String> getColumnVariableNames()
	{
		List<String> names = new ArrayList<>();
		for (Column column : getColumns())
		{
			names.add(column.getName());
		}
		
		return names;
	}
	
	/**
	 * Finds a column in this table that has the provided variable name
	 * @param variableName The name of the column variable
	 * @return A column in this table with the provided name.
	 * @throws NoSuchColumnException If the column can't be found
	 */
	public Column findColumnWithVariableName(String variableName) throws NoSuchColumnException
	{
		for (Column column : getColumns())
		{
			if (column.getName().equalsIgnoreCase(variableName))
				return column;
		}
		
		throw new NoSuchColumnException(this, variableName, true);
	}
	
	/**
	 * Finds a column in this table that has the provided column name
	 * @param columnName The name of a column in the database
	 * @return A column in this table with the provided column name.
	 * @throws NoSuchColumnException If the column can't be found
	 */
	public Column findColumnWithColumnName(String columnName) throws NoSuchColumnException
	{
		for (Column column : getColumns())
		{
			if (column.getColumnName().equalsIgnoreCase(columnName))
				return column;
		}
		
		throw new NoSuchColumnException(this, columnName, false);
	}
	
	/**
	 * Checks whether the table contains a column with the provided name
	 * @param columnName The column name of the searched column
	 * @return Does the table contain a column with the given column name
	 */
	public boolean containsColumn(String columnName)
	{
		for (Column column : getColumns())
		{
			if (column.getColumnName().equalsIgnoreCase(columnName))
				return true;
		}
		
		return false;
	}
	
	/**
	 * Checks whether the table contains a column with the variable name
	 * @param variableName The variable name of the searched column
	 * @return Does the table contain a column with the given variable name
	 */
	public boolean containsColumnForVariable(String variableName)
	{
		for (Column column : getColumns())
		{
			if (column.getName().equalsIgnoreCase(variableName))
				return true;
		}
		
		return false;
	}
	
	/**
	 * Finds all variables in a collection that are associated with this table
	 * @param variables A collection of variables
	 * @return A subset of the provided variables so that the subset contains only variables 
	 * based on this table
	 */
	public List<ColumnVariable> filterTableVariables(Collection<? extends ColumnVariable> variables)
	{
		List<ColumnVariable> tableVars = new ArrayList<>();
		for (ColumnVariable variable : variables)
		{
			if (this.equals(variable.getColumn().getTable()))
				tableVars.add(variable);
		}
		
		return tableVars;
	}
	
	/**
	 * @return a textual description of the table's columns
	 */
	public String getDebugDescription()
	{
		StringBuilder s = new StringBuilder();
		s.append(getDatabaseName());
		s.append(".");
		s.append(getName());
		
		for (Column column : getColumns())
		{
			s.append("\n");
			s.append(column);
		}
		
		return s.toString();
	}
	
	
	// NESTED CLASSES	----------------------
	
	/**
	 * These exceptions are thrown when database table initialisation (reading column 
	 * information, etc) fails
	 * @author Mikko Hilpinen
	 * @since 9.1.2016
	 */
	public static class DatabaseTableInitialisationException extends RuntimeException
	{
		private static final long serialVersionUID = -4211842208696476343L;

		private DatabaseTableInitialisationException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}
	
	/**
	 * These exceptions are thrown when table columns can't be found
	 * @author Mikko Hilpinen
	 * @since 10.1.2016
	 */
	public static class NoSuchColumnException extends RuntimeException
	{
		private static final long serialVersionUID = -1578765040593262050L;

		private NoSuchColumnException(Table table, String name, boolean isVarName)
		{
			super(table + " doesn't contain a column with " + (isVarName ? "variable" : "column") + 
					"name " + name);
		}
		
		private NoSuchColumnException(String message)
		{
			super(message);
		}
	}
}
