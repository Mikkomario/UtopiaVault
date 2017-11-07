package utopia.vault.database;

import utopia.flow.structure.ImmutableList;
import utopia.vault.generics.Column;
import utopia.vault.generics.Table;

/**
 * A selection is used for specifying selected columns from db
 * @author Mikko Hilpinen
 * @since 7.11.2017
 */
public class Selection
{
	// ATTRIBUTES	------------------
	
	/**
	 * A selection that selects all available data
	 */
	public static final Selection ALL = new Selection(true, ImmutableList.empty());
	/**
	 * A selection that selects no data
	 */
	public static final Selection NONE = new Selection(false, ImmutableList.empty());
	
	private final boolean selectAll;
	private final ImmutableList<Column> selectedColumns;
	
	
	// CONSTRUCTOR	-----------------
	
	private Selection(boolean selectAll, ImmutableList<Column> colums)
	{
		this.selectAll = selectAll;
		this.selectedColumns = colums;
	}
	
	/**
	 * Selects certain columns
	 * @param columns The columns that are selected
	 */
	public Selection(ImmutableList<Column> columns)
	{
		this.selectAll = false;
		this.selectedColumns = columns;
	}
	
	/**
	 * Creates a selection that selects all columns from a specific table
	 * @param table the table that is selected
	 */
	public Selection(Table table)
	{
		this.selectAll = false;
		this.selectedColumns = table.getColumns();
	}
	
	/**
	 * Creates a selection that selects certain columns from a specific table
	 * @param table A table
	 * @param firstVarName The name of the first selected variable
	 * @param moreVarNames The names of the rest of the selected variables
	 */
	public Selection(Table table, String firstVarName, String... moreVarNames)
	{
		this.selectAll = false;
		this.selectedColumns = ImmutableList.withValues(firstVarName, moreVarNames).flatMap(name -> 
				table.findColumnWithVariableName(name).stream());
	}
	
	/**
	 * Creates a selection that selects certain columns from a specific table
	 * @param table A table
	 * @param varNames The names of the selected variables
	 */
	public Selection(Table table, ImmutableList<String> varNames)
	{
		this.selectAll = false;
		this.selectedColumns = varNames.flatMap(name -> table.findColumnWithVariableName(name).stream());
	}
	
	
	// IMPLEMENTED METHODS	---------
	
	@Override
	public String toString()
	{
		if (this.selectAll)
			return "*";
		else if (this.selectedColumns.isEmpty())
			return "NULL";
		else
			return this.selectedColumns.toString();
	}
	
	
	// ACCESSORS	----------------
	
	/**
	 * @return Whether all columns should be selected
	 */
	public boolean selectsAll()
	{
		return this.selectAll;
	}
	
	/**
	 * @return Whether no data is selected at all
	 */
	public boolean isEmpty()
	{
		return !selectsAll() && getColumns().isEmpty();
	}
	
	/**
	 * @return The columns that are selected, if they are specified
	 */
	public ImmutableList<Column> getColumns()
	{
		return this.selectedColumns;
	}
	
	
	// OTHER METHODS	-------------
	
	/**
	 * Creates a new, larger selection
	 * @param other Another selection
	 * @return a selection that covers both selections
	 */
	public Selection plus(Selection other)
	{
		if (selectsAll() || other.selectsAll())
			return Selection.ALL;
		else
			return new Selection(getColumns().plusDistinct(other.getColumns()));
	}
}
