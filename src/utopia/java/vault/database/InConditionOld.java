package utopia.java.vault.database;

import java.util.List;

import utopia.java.flow.generics.Value;
import utopia.java.flow.structure.ImmutableList;
import utopia.java.vault.generics.Column;

/**
 * This condition searches for rows where certain column has value in certain group or where 
 * a value can be found within certain column group
 * @author Mikko Hilpinen
 * @since 21.7.2016
 * @deprecated Replaced with a new implementation of {@link InCondition}. This implementation will be removed
 */
public class InConditionOld extends SingleCondition
{
	// ATTRIBUTES	------------------
	
	private Column[] parts;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param inValues The accepted values
	 */
	public InConditionOld(Column column, Value... inValues)
	{
		super(inValues);
		initialise(column, inValues.length);
	}
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param inValues The accepted values
	 */
	public InConditionOld(Column column, List<? extends Value> inValues)
	{
		super(inValues.toArray(new Value[0]));
		initialise(column, inValues.size());
	}
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param inValues The accepted values
	 */
	public InConditionOld(Column column, ImmutableList<Value> inValues)
	{
		super(inValues);
		initialise(column, inValues.size());
	}
	
	/**
	 * Creates a new condition
	 * @param value The searched value
	 * @param inColumns The columns the value is searched from
	 */
	public InConditionOld(Value value, Column... inColumns)
	{
		super(value);
		initialise(inColumns);
	}
	
	/**
	 * Creates a new condition
	 * @param value The searched value
	 * @param inColumns The columns the value is searched from
	 */
	public InConditionOld(Value value, List<? extends Column> inColumns)
	{
		super(value);
		initialise(inColumns.toArray(new Column[0]));
	}
	
	/**
	 * Creates a new condition
	 * @param value The searched value
	 * @param inColumns The columns the value is searched from
	 */
	public InConditionOld(Value value, ImmutableList<Column> inColumns)
	{
		super(value);
		initialise(inColumns.toMutableList().toArray(new Column[0]));
	}
	
	
	// IMPLEMENTED METHODS	-------------

	@Override
	protected String getDebugSqlWithNoParsing()
	{
		return toSql();
	}

	@Override
	public String toSql()
	{
		StringBuilder sql = new StringBuilder();
		appendPart(sql, 0);
		sql.append(" IN (");
		
		for(int i = 1; i < this.parts.length; i++)
		{
			if (i > 1)
				sql.append(", ");
			appendPart(sql, i);
		}
		
		sql.append(")");
		
		return sql.toString();
	}

	
	// OTHER METHODS	----------------
	
	private void appendPart(StringBuilder sql, int index)
	{
		Column column = this.parts[index];
		if (column == null)
			sql.append("?");
		else
			sql.append(column.getColumnNameWithTable());
	}
	
	private void initialise(Column column, int valueAmount)
	{
		specifyValueDataType(column.getType());
		
		this.parts = new Column[valueAmount + 1];
		this.parts[0] = column;
		for (int i = 1; i < this.parts.length; i++)
		{
			this.parts[i] = null;
		}
	}
	
	private void initialise(Column[] columns)
	{
		this.parts = new Column[columns.length + 1];
		this.parts[0] = null;
		for (int i = 1; i < this.parts.length; i++)
		{
			this.parts[i] = columns[i - 1];
		}
	}
}
