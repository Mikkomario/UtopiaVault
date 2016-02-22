package utopia.vault.database;

import utopia.vault.database.Condition.ConditionParseException;
import utopia.vault.generics.Column;
import utopia.vault.generics.Table;

/**
 * A join joins a table into an sql query using a certain condition
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class Join
{
	// ATTRIBUTES	---------------------
	
	private Condition condition;
	private Table table;
	
	
	// CONSTRUCTOR	---------------------
	
	/**
	 * Creates a new join
	 * @param joinedTable the table that is joined
	 * @param joinCondition The condition on which the table is joined
	 */
	public Join(Table joinedTable, Condition joinCondition)
	{
		this.table = joinedTable;
		this.condition = joinCondition;
	}
	
	/**
	 * Creates a new join by linking two columns
	 * @param tableColumn The column in the original table
	 * @param joinedColumn The associated column in the joined table
	 */
	public Join(Column tableColumn, Column joinedColumn)
	{
		this.table = joinedColumn.getTable();
		this.condition = new ComparisonCondition(tableColumn, joinedColumn);
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
	
	
	// OTHER METHODS	------------------
	
	/**
	 * Parses the join into an sql clause like " JOIN table ON condition". The first whitespace 
	 * is included.
	 * @return The join parsed into sql
	 * @throws ConditionParseException If the join condition couldn't be parsed
	 */
	public String toSql() throws ConditionParseException
	{
		StringBuilder sql = new StringBuilder();
		sql.append(" JOIN ");
		sql.append(getJoinedTable().getName());
		sql.append(" ON ");
		sql.append(getJoinCondition().toSql());
		
		return sql.toString();
	}
}