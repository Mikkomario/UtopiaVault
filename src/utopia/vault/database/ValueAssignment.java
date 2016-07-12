package utopia.vault.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.Value;
import utopia.flow.structure.Pair;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.Table;

/**
 * Set is used for parsing a value set in an update sql. Set can assign database values to 
 * match provided values or values of other columns.
 * @author Mikko Hilpinen
 * @since 2.7.2016
 */
public class ValueAssignment implements PreparedSQLClause
{
	// ATTRIBUTES	------------------
	
	private List<Assignment> assignments = new ArrayList<>();
	private boolean removeNulls = false;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new set clause
	 * @param removeNullAssignments Should the assignment filter out any null values
	 * @param values The values set by this clause
	 */
	public ValueAssignment(boolean removeNullAssignments, Collection<? extends ColumnVariable> values)
	{
		this.removeNulls = removeNullAssignments;
		appendValues(values);
	}
	
	/**
	 * Creates a new set clause that assigns the value of a single column to another
	 * @param removeNullAssignments Should the assignment filter out any null values
	 * @param target The column the value is assigned to
	 * @param source The column the value is assigned from
	 */
	public ValueAssignment(boolean removeNullAssignments, Column target, Column source)
	{
		this.removeNulls = removeNullAssignments;
		append(target, source);
	}
	
	/**
	 * Creates a new set clause
	 * @param removeNullAssignments Should the assignment filter out any null values
	 * @param values The values set by this clause
	 */
	public ValueAssignment(boolean removeNullAssignments, ColumnVariable... values)
	{
		this.removeNulls = removeNullAssignments;
		for (ColumnVariable var : values)
		{
			append(var);
		}
	}
	
	/**
	 * Creates a new set clause
	 * @param removeNullAssignments Should the assignment filter out any null values
	 * @param column The column affected by the clause
	 * @param value The value assigned to the column
	 */
	public ValueAssignment(boolean removeNullAssignments, Column column, Value value)
	{
		this.removeNulls = removeNullAssignments;
		append(column, value);
	}
	
	private ValueAssignment(boolean noNulls, List<Assignment> assignments)
	{
		this.removeNulls = noNulls;
		this.assignments.addAll(assignments);
	}

	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toSql()
	{
		return toSql(false);
	}

	@Override
	public Value[] getValues()
	{
		List<Value> values = new ArrayList<>();
		for (Assignment assignment : this.assignments)
		{
			if (assignment.getValue() != null)
				values.add(assignment.getValue());
		}
		
		return values.toArray(new Value[0]);
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * Adds a new value assignment to the set
	 * @param column The target column
	 * @param value The target value
	 */
	public void append(Column column, Value value)
	{
		if (!this.removeNulls || !value.isNull())
			this.assignments.add(new Assignment(column, value));
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param setValue The value assignment added to the clause
	 */
	public void append(ColumnVariable setValue)
	{
		if (!this.removeNulls || !setValue.isNull())
			this.assignments.add(new Assignment(setValue));
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param target The column that will be affected by the set
	 * @param source The column the value is taken from
	 */
	public void append(Column target, Column source)
	{
		this.assignments.add(new Assignment(target, source));
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
		for (Pair<Column, Column> columnPair : setColumns)
		{
			append(columnPair.getFirst(), columnPair.getSecond());
		}
	}
	
	/**
	 * Creates a set clause based on this assignment. A whitespace is added before the string. 
	 * An example clause may be: " SET table1.column1=?, table1.column2=?"
	 * @return A set clause based on this assignment object.
	 */
	public String toSetClause()
	{
		return " SET " + toSql();
	}
	
	/**
	 * Creates an insert statement based on this assignment. No filtering is done and all 
	 * assignments will be parsed whether they are suitable for an insert or not (Eg. doesn't 
	 * check for null values, incomplete inserts, auto-increment key assignments).<br>
	 * An example result could be: 'INSERT INTO table1 (column1, column2, column3) VALUES (?, ?, ?)'. 
	 * It is not advised to use column to column assignments in inserts.
	 * @param targetTable The table the insert is made for
	 * @return The insert sql. No whitespace is added before the "INSERT" -statement.
	 */
	public String toInsertClause(Table targetTable)
	{
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(targetTable.getName());
		sql.append(" (");
		
		// Adds all targeted columns, prepares the value sql as well
		StringBuilder values = new StringBuilder();
		
		boolean isFirst = true;
		for (Assignment assignment : this.assignments)
		{
			if (isFirst)
				isFirst = false;
			else
			{
				sql.append(", ");
				values.append(", ");
			}
			
			sql.append(assignment.getTargetColumn().getColumnName());
			values.append(assignment.getSecondPartSQL(false));
		}
		
		sql.append(") VALUES (");
		sql.append(values);
		sql.append(")");
		
		return sql.toString();
	}
	
	/**
	 * @return Whether this assignment is empty (contains no assignments)
	 */
	public boolean isEmpty()
	{
		return this.assignments.isEmpty();
	}
	
	/**
	 * Checks whether the assignment contains a non-null assignment for each of the target table columns
	 * @param targetTable The targeted table
	 * @return Does the assignment contain an assignment for each of the required columns of the table
	 */
	public boolean containsRequiredColumns(Table targetTable)
	{
		for (Column column : targetTable.getColumns())
		{
			if (column.requiredInInsert() && !containsColumn(column, true))
				return false;
		}
		
		return true;
	}
	
	/**
	 * Checks whether the assignment contains an assignment for the provided column
	 * @param column A column
	 * @param ignoreNullAssignments Should null value assignments be ignored in this search
	 * @return Does the assignment contain an assignment for the column
	 */
	public boolean containsColumn(Column column, boolean ignoreNullAssignments)
	{
		for (Assignment assignment : this.assignments)
		{
			// Null values may be filtered
			if (assignment.getTargetColumn().equals(column))
			{
				if (!ignoreNullAssignments || 
						(assignment.getValue() == null || !assignment.getValue().isNull()))
					return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided tables
	 * @param tables The tables
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTables(Collection<? extends Table> tables, boolean removeAutoIncrementKeys)
	{
		List<Assignment> filtered = new ArrayList<>();
		for (Assignment assignment : this.assignments)
		{
			// The target column table must match one of the targeted tables
			if (tables.contains(assignment.getTargetColumn().getTable()))
			{
				// Auto-increment keys may be filtered as well
				if (!removeAutoIncrementKeys || !assignment.getTargetColumn().usesAutoIncrementIndexing())
					filtered.add(assignment);
			}
		}
		
		return new ValueAssignment(this.removeNulls, filtered);
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided tables
	 * @param table The primary table
	 * @param joins The joins for the joined tables
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTables(Table table, Join[] joins, boolean removeAutoIncrementKeys)
	{
		List<Table> tables = new ArrayList<>();
		tables.add(table);
		if (joins != null)
		{
			for (Join join : joins)
			{
				tables.add(join.getJoinedTable());
			}
		}
		
		return filterToTables(tables, removeAutoIncrementKeys);
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided table
	 * @param table The table
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTable(Table table, boolean removeAutoIncrementKeys)
	{
		List<Table> tables = new ArrayList<>();
		tables.add(table);
		return filterToTables(tables, removeAutoIncrementKeys);
	}
	
	/**
	 * @return A debugging string based on this assignment
	 */
	public String getDebugDescription()
	{
		return toSql(true);
	}
	
	private String toSql(boolean debugVersion)
	{
		StringBuilder sql = new StringBuilder();
		
		boolean isFirst = true;
		for (Assignment assignment : this.assignments)
		{
			if (isFirst)
				isFirst = false;
			else
				sql.append(", ");
			
			sql.append(assignment.toSQL(debugVersion));
		}
		
		return sql.toString();
	}
	
	
	// NESTED CLASSES	-------------------
	
	private static class Assignment
	{
		// ATTRIBUTES	-------------------
		
		private Column targetColumn;
		private Column sourceColumn = null;
		private Value value = null;
		
		
		// CONSTRUCTOR	-------------------
		
		public Assignment(Column targetColumn, Column sourceColumn)
		{
			this.targetColumn = targetColumn;
			this.sourceColumn = sourceColumn;
		}
		
		public Assignment(Column targetColumn, Value value)
		{
			this.targetColumn = targetColumn;
			this.value = value;
		}
		
		public Assignment(ColumnVariable var)
		{
			this.targetColumn = var.getColumn();
			this.value = var.getValue();
		}
		
		// IMPLEMENTED METHODS	----------
		
		@Override
		public String toString()
		{
			return toSQL(true);
		}
		
		
		// ACCESSORS	------------------
		
		public Column getTargetColumn()
		{
			return this.targetColumn;
		}
		
		public Value getValue()
		{
			return this.value;
		}
		
		
		// OTHER METHODS	--------------
		
		public String getSecondPartSQL(boolean debugVersion)
		{
			if (getValue() == null)
				return this.sourceColumn.getColumnNameWithTable();
			else if (debugVersion)
				return this.value.getDescription();
			else
				return "?";
		}
		
		public String toSQL(boolean debugVersion)
		{
			StringBuilder s = new StringBuilder();
			s.append(getTargetColumn().getColumnNameWithTable());
			s.append("=");
			s.append(getSecondPartSQL(debugVersion));
			
			return s.toString();
		}
	}
}
