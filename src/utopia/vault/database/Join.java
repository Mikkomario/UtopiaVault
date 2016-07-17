package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.vault.generics.Column;
import utopia.vault.generics.Table;

/**
 * A join joins a table into an sql query using a certain condition
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class Join implements PreparedSQLClause
{
	// ATTRIBUTES	---------------------
	
	private Condition condition;
	private Table table;
	private JoinType type;
	
	
	// CONSTRUCTOR	---------------------
	
	/**
	 * Creates a new inner join
	 * @param joinedTable the table that is joined
	 * @param joinCondition The condition on which the table is joined
	 */
	public Join(Table joinedTable, Condition joinCondition)
	{
		this.table = joinedTable;
		this.condition = joinCondition;
		this.type = JoinType.INNER;
	}
	
	/**
	 * Creates a new inner join by linking two columns
	 * @param tableColumn The column in the original table
	 * @param joinedColumn The associated column in the joined table
	 */
	public Join(Column tableColumn, Column joinedColumn)
	{
		this.table = joinedColumn.getTable();
		this.condition = new ComparisonCondition(tableColumn, joinedColumn);
		this.type = JoinType.INNER;
	}
	
	/**
	 * Creates a new join
	 * @param joinedTable the table that is joined
	 * @param joinCondition The condition on which the table is joined
	 * @param type The type of the join
	 */
	public Join(Table joinedTable, Condition joinCondition, JoinType type)
	{
		this.table = joinedTable;
		this.condition = joinCondition;
		this.type = type;
	}
	
	/**
	 * Creates a new join by linking two columns
	 * @param tableColumn The column in the original table
	 * @param joinedColumn The associated column in the joined table
	 * @param type The type of the join
	 */
	public Join(Column tableColumn, Column joinedColumn, JoinType type)
	{
		this.table = joinedColumn.getTable();
		this.condition = new ComparisonCondition(tableColumn, joinedColumn);
		this.type = type;
	}
	
	// TODO: Create reference joins
	
	
	// IMPLEMENTED METHODS	------------
	
	@Override
	public Value[] getValues()
	{
		return this.condition.getValues();
	}
	
	/**
	 * Parses the join into an sql clause like " JOIN table ON condition". The first whitespace 
	 * is included.
	 * @return The join parsed into sql
	 * @throws StatementParseException If the join condition couldn't be parsed
	 */
	@Override
	public String toSql() throws StatementParseException
	{
		StringBuilder sql = new StringBuilder();
		sql.append(this.type.toSql());
		sql.append(getJoinedTable().getName());
		sql.append(" ON ");
		sql.append(getJoinCondition().toSql());
		
		return sql.toString();
	}
	
	
	// ACCESSORS	--------------------
	
	/**
	 * @return The table from which rows are joined into the query
	 */
	public Table getJoinedTable()
	{
		return this.table;
	}
	
	/**
	 * @return The condition on which the rows are joined to the query
	 */
	public Condition getJoinCondition()
	{
		return this.condition;
	}
	
	
	// ENUMERATIONS		-----------------
	
	/**
	 * These are the different methods for joining two tables together
	 * @author Mikko Hilpinen
	 * @since 2.3.2016
	 */
	public static enum JoinType
	{
		/**
		 * Includes all rows from the primary table + connected rows from the joined table
		 */
		LEFT(" LEFT JOIN "),
		/**
		 * Includes all rows from the joined table + connected rows from the primary table
		 */
		RIGHT(" RIGHT JOIN "),
		/**
		 * Includes only connected rows from both tables
		 */
		INNER(" INNER JOIN ");
		
		
		// ATTRIBUTES	----------------
		
		private final String sql;
		
		
		// CONSTRUCTOR	----------------
		
		private JoinType(String sql)
		{
			this.sql = sql;
		}
		
		
		// IMPLEMENTED METHODS	---------
		
		@Override
		public String toString()
		{
			return toSql();
		}
		
		
		// OTHER METHODS	--------------
		
		/**
		 * @return The sql syntax for this join. Includes white spaces around the sql. For 
		 * example: " LEFT JOIN "
		 */
		public String toSql()
		{
			return this.sql;
		}
	}
}
