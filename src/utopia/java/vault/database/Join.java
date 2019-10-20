package utopia.java.vault.database;

import utopia.java.flow.generics.Value;
import utopia.java.flow.structure.ImmutableList;
import utopia.java.vault.generics.Column;
import utopia.java.vault.generics.NoSuchReferenceException;
import utopia.java.vault.generics.Table;
import utopia.java.vault.generics.TableReference;

/**
 * A join joins a table into an sql query using a certain condition
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class Join implements PreparedSQLClause
{
	// ATTRIBUTES	---------------------
	
	private Condition condition;
	private Table joinedTable;
	private JoinType type;
	
	
	// CONSTRUCTOR	---------------------
	
	/**
	 * Creates a new inner join
	 * @param joinedTable the table that is joined
	 * @param joinCondition The condition on which the table is joined
	 */
	public Join(Table joinedTable, Condition joinCondition)
	{
		this.joinedTable = joinedTable;
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
		this.joinedTable = joinedColumn.getTable();
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
		this.joinedTable = joinedTable;
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
		this.joinedTable = joinedColumn.getTable();
		this.condition = new ComparisonCondition(tableColumn, joinedColumn);
		this.type = type;
	}
	
	/**
	 * Creates a new join condition based on the provided table reference. The referenced 
	 * table is considered to be the joined table in this case
	 * @param reference A table reference
	 * @param type The type of the join condition
	 */
	public Join(TableReference reference, JoinType type)
	{
		this.joinedTable = reference.getReferencedColumn().getTable();
		this.condition = new ComparisonCondition(reference.getReferencingColumn(), 
				reference.getReferencedColumn());
		this.type = type;
	}
	
	/**
	 * Creates a new join between the two tables using table references
	 * @param from The primary table
	 * @param to The joined table
	 * @param type The join type
	 * @throws NoSuchReferenceException If there wasn't a single reference between the two tables
	 */
	public Join(Table from, Table to, JoinType type) throws NoSuchReferenceException
	{
		this.joinedTable = to;
		this.type = type;
		this.condition = getReferenceCondition(from, to);
	}
	
	/**
	 * Creates a new inner join between two tables using table references
	 * @param from The primary table
	 * @param to The joined table
	 * @throws NoSuchReferenceException If there wasn't a single reference between the two tables
	 */
	public Join(Table from, Table to) throws NoSuchReferenceException
	{
		this.joinedTable = to;
		this.type = JoinType.INNER;
		this.condition = getReferenceCondition(from, to);
	}
	
	/**
	 * Creates multiple joins that link the provided tables together. The first table will be 
	 * joined with the second, which will be joined with the third, which will be joined 
	 * with the fourth and so on.
	 * @param tables The tables that are linked / joined
	 * @return A collection of joins
	 * @throws NoSuchReferenceException If there wasn't a reference between some two tables
	 */
	public static ImmutableList<Join> createReferenceJoins(Table... tables) throws NoSuchReferenceException
	{
		if (tables.length < 2)
			return ImmutableList.empty();
		else
		{
			Join[] joins = new Join[tables.length - 1];
			for (int i = 0; i < joins.length; i++)
			{
				joins[i] = new Join(tables[i], tables[i + 1]);
			}
			
			return ImmutableList.of(joins);
		}
	}
	
	
	// IMPLEMENTED METHODS	------------
	
	@Override
	public String toString()
	{
		return describe();
	}
	
	@Override
	public ImmutableList<Value> getValues()
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
		return this.joinedTable;
	}
	
	/**
	 * @return The condition on which the rows are joined to the query
	 */
	public Condition getJoinCondition()
	{
		return this.condition;
	}
	
	
	// OTHER METHODS	---------------
	
	/**
	 * Creates a new join that has an extended condition
	 * @param extension The extension added to the already existing join condition using AND
	 * @return The new join
	 */
	public Join withExtendedCondition(Condition extension)
	{
		return new Join(getJoinedTable(), getJoinCondition().and(extension), this.type);
	}
	
	private static Condition getReferenceCondition(Table from, Table to) throws NoSuchReferenceException
	{
		// Finds possible references from the first table to the joined table
		ImmutableList<TableReference> references = from.getReferencesToTable(to);
		
		// If there weren't any references, tries the other way instead
		if (references.isEmpty())
			references = to.getReferencesToTable(from);
		
		// If there aren't any references, fails
		if (references.isEmpty())
			throw new NoSuchReferenceException(from, to);
		
		// In case there is a single reference only, the columns are compared
		if (references.size() == 1)
			return new ComparisonCondition(references.head().getReferencingColumn(), 
					references.head().getReferencedColumn());
		// Otherwise each reference is joined
		else
		{
			ImmutableList<Condition> conditions = references.map(ref -> 
					new ComparisonCondition(ref.getReferencingColumn(), ref.getReferencedColumn()));
			return CombinedCondition.combineConditions(CombinedCondition.CombinationOperator.OR, conditions).get();
		}
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
