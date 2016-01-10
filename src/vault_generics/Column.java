package vault_generics;

import vault_generics.VariableNameMapping.NoVariableForColumnException;
import flow_generics.Value;
import flow_generics.VariableDeclaration;

/**
 * Column is a description of a single column in a table
 * @author Mikko Hilpinen
 * @since 30.5.2015
 */
public class Column extends VariableDeclaration
{
	// ATTRIBUTES	----------------------
	
	private String columnName;
	private Table table;
	private boolean autoIncrement, primary, nullAllowed;
	private Value defaultValue;
	private SqlDataType type;
	
	
	// CONSTRUCTOR	----------------------
	
	/**
	 * Creates a new column representation
	 * @param table The database table that contains this column
	 * @param columnName The name of the column
	 * @param primary Does the column hold a primary key
	 * @param autoIncrement Does the column use auto-increment indexing
	 * @param type The data type of the column's value
	 * @param nullAllowed Is null allowed in this column
	 * @param defaultValue The default value used for this column. The value will be cast 
	 * to the correct type
	 * @throws NoVariableForColumnException If the column name couldn't be mapped to a 
	 * variable name
	 */
	public Column(Table table, String columnName, SqlDataType type, boolean nullAllowed, 
			boolean primary, boolean autoIncrement, Value defaultValue) throws NoVariableForColumnException
	{
		super(table.getNameMapping().getVariableName(columnName), type);
		
		this.type = type;
		this.table = table;
		this.columnName = columnName;
		this.primary = primary;
		this.autoIncrement = autoIncrement;
		this.nullAllowed = nullAllowed;
		this.defaultValue = defaultValue.castTo(type);
	}
	
	
	// IMPLEMENTED METHODS	---------------
	
	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder(super.toString());
		if (isPrimary())
			s.append(" PRI");
		if (!nullAllowed())
			s.append(" not null");
		if (usesAutoIncrementIndexing())
			s.append(" auto-increment");
		if (getDefaultValue() != null)
			s.append(" default = " + getDefaultValue().toString());
		
		return s.toString();
	}
	
	
	// ACCESSORS	---------------
	
	/**
	 * @return The table the column is in
	 */
	public Table getTable()
	{
		return this.table;
	}
	
	/**
	 * @return The name of the column
	 */
	public String getColumnName()
	{
		return this.columnName;
	}
	
	/**
	 * @return Does the column hold a primary key (default = false)
	 */
	public boolean isPrimary()
	{
		return this.primary;
	}
	
	/**
	 * @return Does the column use auto-increment indexing (default = false)
	 */
	public boolean usesAutoIncrementIndexing()
	{
		return this.autoIncrement;
	}
	
	/**
	 * @return Is null value allowed in this column (default = true)
	 */
	public boolean nullAllowed()
	{
		return this.nullAllowed;
	}
	
	/**
	 * @return The default value used for this column (default = null)
	 */
	public Value getDefaultValue()
	{
		return this.defaultValue;
	}
	
	/**
	 * @return The data type of the column in sql type format
	 */
	public SqlDataType getSqlType()
	{
		return this.type;
	}
}
