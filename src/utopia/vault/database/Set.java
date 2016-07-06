package utopia.vault.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.Value;
import utopia.flow.structure.Pair;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;

/**
 * Set is used for parsing a value set in an update sql. Set can assign database values to 
 * match provided values or values of other columns.
 * @author Mikko Hilpinen
 * @since 2.7.2016
 */
public class Set implements PreparedSQLClause
{
	// ATTRIBUTES	------------------
	
	private List<Pair<Column, Value>> valueSet = new ArrayList<>();
	private List<Pair<Column, Column>> columnSet = new ArrayList<>();
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new set clause
	 * @param values The values set by this clause
	 */
	public Set(Collection<? extends ColumnVariable> values)
	{
		appendValues(values);
	}
	
	/**
	 * Creates a new set clause that assigns the value of a single column to another
	 * @param target The column the value is assigned to
	 * @param source The column the value is assigned from
	 */
	public Set(Column target, Column source)
	{
		append(target, source);
	}
	
	/**
	 * Creates a new set clause
	 * @param values The values set by this clause
	 */
	public Set(ColumnVariable... values)
	{
		for (ColumnVariable var : values)
		{
			append(var);
		}
	}
	
	/**
	 * Creates a new set clause
	 * @param column The column affected by the clause
	 * @param value The value assigned to the column
	 */
	public Set(Column column, Value value)
	{
		append(column, value);
	}

	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toSql()
	{
		StringBuilder sql = new StringBuilder(" SET ");
		
		boolean isFirst = true;
		for (Pair<Column, Value> valueToColumn : this.valueSet)
		{
			if (isFirst)
				isFirst = false;
			else
				sql.append(", ");
			
			sql.append(valueToColumn.getFirst().getColumnNameWithTable());
			sql.append("=?");
		}
		for (Pair<Column, Column> columnToColumn : this.columnSet)
		{
			if (isFirst)
				isFirst = false;
			else
				sql.append(", ");
			
			sql.append(columnToColumn.getFirst().getColumnNameWithTable());
			sql.append("=");
			sql.append(columnToColumn.getSecond().getColumnNameWithTable());
		}
		
		return sql.toString();
	}

	@Override
	public Value[] getValues()
	{
		Value[] values = new Value[this.valueSet.size()];
		for (int i = 0; i < values.length; i++)
		{
			values[i] = this.valueSet.get(i).getSecond();
		}
		
		return values;
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * Adds a new value assignment to the set
	 * @param column The target column
	 * @param value The target value
	 */
	public void append(Column column, Value value)
	{
		this.valueSet.add(new Pair<>(column, value));
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param setValue The value assignment added to the clause
	 */
	public void append(ColumnVariable setValue)
	{
		append(setValue.getColumn(), setValue.getValue());
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param target The column that will be affected by the set
	 * @param source The column the value is taken from
	 */
	public void append(Column target, Column source)
	{
		this.columnSet.add(new Pair<>(target, source));
	}
	
	/**
	 * Adds multiple value assignments to the set
	 * @param setValues The value assignments added to the set
	 */
	public void appendValues(Collection<? extends ColumnVariable> setValues)
	{
		for (ColumnVariable var : setValues)
		{
			append(var);
		}
	}
	
	/**
	 * Adds multiple value assignments to the set
	 * @param setColumns The target and source columns used in the set
	 */
	public void appendColumns(Collection<? extends Pair<Column, Column>> setColumns)
	{
		this.columnSet.addAll(setColumns);
	}
}
