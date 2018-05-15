package utopia.vault.database;

import java.util.Collection;

import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.Pair;
import utopia.flow.structure.Option;
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
	
	private ImmutableList<Assignment> assignments = ImmutableList.empty();
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
	 * Creates a new set clause
	 * @param removeNullAssignments Should the assignment filter out any null values
	 * @param values The values set by this clause
	 */
	public ValueAssignment(boolean removeNullAssignments, ImmutableList<? extends ColumnVariable> values)
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
	
	private ValueAssignment(ImmutableList<Assignment> assignments, boolean noNulls)
	{
		this.removeNulls = noNulls;
		this.assignments = assignments;
	}

	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toSql()
	{
		return toSql(false);
	}

	@Override
	public ImmutableList<Value> getValues()
	{
		return this.assignments.flatMap(a -> a.getValue());
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
			this.assignments = this.assignments.plus(new Assignment(column, value));
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param setValue The value assignment added to the clause
	 */
	public void append(ColumnVariable setValue)
	{
		if (!this.removeNulls || !setValue.isNull())
			this.assignments = this.assignments.plus(new Assignment(setValue));
	}
	
	/**
	 * Adds a new value assignment to the set
	 * @param target The column that will be affected by the set
	 * @param source The column the value is taken from
	 */
	public void append(Column target, Column source)
	{
		this.assignments = this.assignments.plus(new Assignment(target, source));
	}
	
	/**
	 * Adds multiple value assignments to the set
	 * @param setValues The value assignments added to the set
	 */
	public void appendValues(Iterable<? extends ColumnVariable> setValues)
	{
		for (ColumnVariable var : setValues)
		{
			append(var);
		}
	}
	
	/**
	 * Adds multiple value assignments to the set
	 * @param setValues The value assignments added to the set
	 */
	public void appendValues(ImmutableList<? extends ColumnVariable> setValues)
	{
		this.assignments = this.assignments.plus(setValues.map(s -> new Assignment(s)));
	}
	
	/**
	 * Adds multiple value assignments to the set
	 * @param setColumns The target and source columns used in the set
	 */
	public void appendColumns(ImmutableList<? extends Pair<Column, Column>> setColumns)
	{
		this.assignments = this.assignments.plus(setColumns.map(pair -> new Assignment(pair.getFirst(), pair.getSecond())));
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
		return this.assignments.exists(a -> a.getTargetColumn().equals(column) && 
				(!ignoreNullAssignments || (a.getValue().exists(value -> !value.isNull()))));
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided tables
	 * @param tables The tables
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTables(ImmutableList<? extends Table> tables, boolean removeAutoIncrementKeys)
	{
		// The target column table must match one of the targeted tables
		// Auto-increment keys may be filtered as well
		return new ValueAssignment(this.assignments.filter(a -> tables.contains(a.getTargetColumn().getTable()) && 
				(!removeAutoIncrementKeys || !a.getTargetColumn().usesAutoIncrementIndexing())), this.removeNulls);
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided tables
	 * @param table The primary table
	 * @param joins The joins for the joined tables
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTables(Table table, ImmutableList<Join> joins, boolean removeAutoIncrementKeys)
	{
		return filterToTables((joins == null ? ImmutableList.empty() : 
				joins.map(join -> join.getJoinedTable()).plus(table)), removeAutoIncrementKeys);
	}
	
	/**
	 * Filters out all assignments that don't belong to the provided table
	 * @param table The table
	 * @param removeAutoIncrementKeys Should auto-increment keys be removed as well
	 * @return A filtered version of this assignment
	 */
	public ValueAssignment filterToTable(Table table, boolean removeAutoIncrementKeys)
	{
		return filterToTables(ImmutableList.withValue(table), removeAutoIncrementKeys);
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
		private Option<Column> sourceColumn = Option.none();
		private Option<Value> value = Option.none();
		
		
		// CONSTRUCTOR	-------------------
		
		public Assignment(Column targetColumn, Column sourceColumn)
		{
			this.targetColumn = targetColumn;
			this.sourceColumn = Option.some(sourceColumn);
		}
		
		public Assignment(Column targetColumn, Value value)
		{
			this.targetColumn = targetColumn;
			this.value = Option.some(value);
		}
		
		public Assignment(ColumnVariable var)
		{
			this.targetColumn = var.getColumn();
			this.value = Option.some(var.getValue());
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
		
		public Option<Value> getValue()
		{
			return this.value;
		}
		
		
		// OTHER METHODS	--------------
		
		public String getSecondPartSQL(boolean debugVersion)
		{
			if (getValue().isEmpty())
				return this.sourceColumn.get().getColumnNameWithTable();
			else if (debugVersion)
				return this.value.get().getDescription();
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
