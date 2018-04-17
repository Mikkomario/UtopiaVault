package utopia.vault.generics;

import utopia.flow.generics.ModelDeclaration;
import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.ImmutableMap;
import utopia.flow.util.Lazy;
import utopia.flow.util.Option;

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
	private ColumnInitialiser columnInitialiser;
	private Option<TableReferenceReader> referenceReader;
	
	private final Lazy<ImmutableList<Column>> columns = new Lazy<>(this::readColumns);
	private final Lazy<Option<Column>> primaryColumn = new Lazy<>(() -> getColumns().find(column -> column.isPrimary()));
	private final Lazy<ModelDeclaration> declaration = new Lazy<>(() -> new ModelDeclaration(ImmutableList.of(getColumns())));
	private ImmutableMap<Table, ImmutableList<TableReference>> references = ImmutableMap.empty();
	
	
	// CONSTRUCTOR	-------------
	
	/**
	 * Creates a new database table
	 * @param databaseName The name of the database the table uses
	 * @param name The name of the table
	 * @param nameMapping The name mapping the table uses
	 * @param columnInitialiser The initialiser that is able to initialise the table's column data 
	 * when necessary
	 * @param referenceReader The instance that generates references for this table when 
	 * necessary (Optional)
	 */
	public Table(String databaseName, String name, VariableNameMapping nameMapping, 
			ColumnInitialiser columnInitialiser, Option<TableReferenceReader> referenceReader)
	{
		this.databaseName = databaseName;
		this.name = name;
		this.nameMapping = nameMapping;
		this.columnInitialiser = columnInitialiser;
		this.referenceReader = referenceReader;
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
	 * @throws TableInitialisationException If the columns couldn't be initialised, 
	 * for some reason
	 */
	public ImmutableList<Column> getColumns() throws TableInitialisationException
	{
		// Reads the column data from the database when first requested
		return this.columns.get();
	}
	
	/**
	 * Finds a set of columns that are represented by the provided variable names
	 * @param variableNames The names of the variables whose columns are requested
	 * @return A list of columns containing a column for each of the provided variable names 
	 * that has a corresponding column in this table
	 */
	public ImmutableList<Column> getVariableColumns(ImmutableList<String> variableNames)
	{
		return variableNames.flatMap(name -> getColumns().find(column -> column.getName().equalsIgnoreCase(name)));
	}
	
	/**
	 * Finds a set of columns that are represented by the provided variable names
	 * @param variableNames The names of the variables whose columns are requested
	 * @return A list of columns containing a column for each of the provided variable names 
	 * that has a corresponding column in this table
	 */
	public ImmutableList<Column> getVariableColumns(String... variableNames)
	{
		return getVariableColumns(ImmutableList.of(variableNames));
	}
	
	/**
	 * @return The primary column in this table.
	 * @throws NoSuchColumnException If there is no primary column in this table
	 */
	public Column getPrimaryColumn() throws NoSuchColumnException
	{
		Option<Column> primary = findPrimaryColumn();
		if (primary.isEmpty())
			throw new NoSuchColumnException("Table " + this + " doesn't have a primary column");
		else
			return primary.get();
	}
	
	/**
	 * @return The primary column in this table. Null if there is no primary column
	 */
	public Option<Column> findPrimaryColumn()
	{
		return this.primaryColumn.get();
	}
	
	/**
	 * @return The model declaration representation of this table
	 */
	public ModelDeclaration toModelDeclaration()
	{
		return this.declaration.get();
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * Finds all the references from this table to the other table
	 * @param table The table this table may reference
	 * @return The references from this table to the other table
	 * @throws TableInitialisationException If the references couldn't be read / generated 
	 * for some reason
	 */
	public ImmutableList<TableReference> getReferencesToTable(Table table) throws TableInitialisationException
	{
		Option<ImmutableList<TableReference>> references = this.references.getOption(table);
		
		// The reference data may be read if it wasn't already
		if (references.isEmpty())
		{
			ImmutableList<TableReference> newReferences = this.referenceReader.map(reader -> 
					reader.getReferencesBetween(this, table)).getOrElse(ImmutableList.empty());
			
			this.references = this.references.plus(table, newReferences);
			return newReferences;
		}
		else
			return references.get();
	}
	
	/**
	 * Defines the references from this table to another table. This will overwrite any existing 
	 * reference to the targeted table
	 * @param to The table that is referenced
	 * @param references References to that table
	 */
	public void setReferences(Table to, TableReference... references)
	{
		setReferences(to, ImmutableList.of(references));
	}
	
	/**
	 * Defines the references from this table to another table. This will overwrite any existing 
	 * reference to the targeted table
	 * @param to The table that is referenced
	 * @param references References to that table
	 */
	public void setReferences(Table to, ImmutableList<TableReference> references)
	{
		this.references = this.references.plus(to, references);
	}
	
	/**
	 * Checks whether a table references another table
	 * @param table The table that may be referenced from this table
	 * @return Does this table contain a reference to the targeted table
	 * @throws TableInitialisationException If reference data couldn't be generated / read
	 */
	public boolean references(Table table) throws TableInitialisationException
	{
		return !getReferencesToTable(table).isEmpty();
	}
	
	/**
	 * @return Does the table's primary key use auto increment indexing
	 */
	public boolean usesAutoIncrementIndexing()
	{
		return findPrimaryColumn().exists(column -> column.usesAutoIncrementIndexing());
	}
	
	/**
	 * @return A list containing the name of each column in the table
	 */
	public ImmutableList<String> getColumnNames()
	{
		return getColumns().map(c -> c.getColumnName());
	}
	
	/**
	 * @return A list containing the variable names of the columns in the table
	 */
	public ImmutableList<String> getColumnVariableNames()
	{
		return getColumns().map(c -> c.getName());
	}
	
	/**
	 * Finds a column in this table that has the provided variable name
	 * @param variableName The name of the column variable
	 * @return A column in this table with the provided name. None if no such column exists.
	 */
	public Option<Column> findColumnWithVariableName(String variableName)
	{
		return getColumns().find(c -> c.getName().equalsIgnoreCase(variableName));
	}
	
	/**
	 * Finds a column in this table that has the provided column name
	 * @param columnName The name of a column in the database
	 * @return A column in this table with the provided column name. None if no such column exists.
	 */
	public Option<Column> findColumnWithColumnName(String columnName)
	{
		return getColumns().find(c -> c.getColumnName().equalsIgnoreCase(columnName));
	}
	
	/**
	 * @param varName The name of the variable
	 * @return A column matching the variable
	 * @throws NoSuchColumnException If no such column exists
	 */
	public Column getColumnWithVariableName(String varName) throws NoSuchColumnException
	{
		Option<Column> column = findColumnWithVariableName(varName);
		if (column.isDefined())
			return column.get();
		else
			throw new NoSuchColumnException(this, varName, true);
	}
	
	/**
	 * @param columnName The name of the column
	 * @return A column matching the name
	 * @throws NoSuchColumnException If no such column exists
	 */
	public Column getColumnWithColumnName(String columnName) throws NoSuchColumnException
	{
		Option<Column> column = findColumnWithColumnName(columnName);
		if (column.isDefined())
			return column.get();
		else
			throw new NoSuchColumnException(this, columnName, false);
	}
	
	/**
	 * Checks whether the table contains a column with the provided name
	 * @param columnName The column name of the searched column
	 * @return Does the table contain a column with the given column name
	 */
	public boolean containsColumn(String columnName)
	{
		return getColumns().exists(c -> c.getColumnName().equalsIgnoreCase(columnName));
	}
	
	/**
	 * Checks whether the table contains a column with the variable name
	 * @param variableName The variable name of the searched column
	 * @return Does the table contain a column with the given variable name
	 */
	public boolean containsColumnForVariable(String variableName)
	{
		return getColumns().exists(c -> c.getName().equalsIgnoreCase(variableName));
	}
	
	/**
	 * Finds all variables in a collection that are associated with this table
	 * @param variables A collection of variables
	 * @return A subset of the provided variables so that the subset contains only variables 
	 * based on this table
	 */
	public ImmutableList<ColumnVariable> filterTableVariables(ImmutableList<ColumnVariable> variables)
	{
		return variables.filter(var -> equals(var.getColumn().getTable()));
	}
	
	/**
	 * Finds an index for this table from the provided set of variables
	 * @param vars a set of variables
	 * @return An index for this table. An empty value if the set didn't contain the index
	 */
	public Value getIndexFrom(ImmutableList<? extends ColumnVariable> vars)
	{
		return vars.find(var -> var.getColumn().isPrimary() && var.getColumn().getTable().equals(this)).map(
				var -> var.getValue()).getOrElse(Value.EMPTY);
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
	
	private ImmutableList<Column> readColumns()
	{
		ImmutableList<Column> columns = this.columnInitialiser.generateColumns(this);
		// Also initialises the mappings
		getNameMapping().addMappingForEachColumnWherePossible(columns);
		
		return columns;
	}
	
	
	// NESTED CLASSES	----------------------
	
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
		
		/**
		 * Creates a new exception
		 * @param message The exception message
		 */
		public NoSuchColumnException(String message)
		{
			super(message);
		}
	}
}
