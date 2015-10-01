package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A where clause is a set of where conditions that must be met. The clause can be used in 
 * sql operations. The clause is immutable once created.
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public class WhereClause
{
	// ATTRIBUTES	-----------------
	
	private List<WhereCondition> conditions;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a new empty where clause
	 */
	public WhereClause()
	{
		this.conditions = new ArrayList<>();
	}

	/**
	 * Creates a where clause with a single condition
	 * @param condition The condition in the where clause
	 */
	public WhereClause(WhereCondition condition)
	{
		this.conditions = new ArrayList<>();
		this.conditions.add(condition);
	}
	
	/**
	 * Creates a where clause with multiple conditions
	 * @param conditions The conditions in this where clause
	 */
	public WhereClause(Collection<? extends WhereCondition> conditions)
	{
		this.conditions = new ArrayList<>();
		this.conditions.addAll(conditions);
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Returns a combined the where clause. This doesn't change this clause at all.
	 * @param condition An additional condition
	 * @return A where clause with the current conditions and the new one
	 */
	public WhereClause and(WhereCondition condition)
	{
		if (!this.conditions.contains(condition))
		{
			WhereClause newClause = new WhereClause(this.conditions);
			newClause.conditions.add(condition);
			return newClause;
		}
		else
			return this;
	}
	
	/**
	 * Returns a combined where clause. This doesn't change either of these clauses.
	 * @param other Another where clause
	 * @return A combination of the two clauses
	 */
	public WhereClause and(WhereClause other)
	{
		boolean newConditionsAdded = false;
		List<WhereCondition> combinedConditions = new ArrayList<>();
		combinedConditions.addAll(this.conditions);
		
		for (WhereCondition condition : other.conditions)
		{
			if (!this.conditions.contains(condition))
			{
				combinedConditions.add(condition);
				newConditionsAdded = true;
			}
		}
		
		if (newConditionsAdded)
			return new WhereClause(combinedConditions);
		else
			return this;
	}
	
	/**
	 * Parses an sql statement for this where clause. Values will be left as '?' for future 
	 * preparation
	 * @param targetTable The table the clause is used on
	 * @return An sql statement with '?' as placeholders for values
	 */
	public String toSql(DatabaseTable targetTable)
	{
		if (this.conditions.isEmpty())
			return "";
		
		StringBuilder sql = new StringBuilder(" WHERE");
		
		boolean isFirst = true;
		for (WhereCondition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" AND");
			sql.append(condition.toSql(targetTable));
			isFirst = false;
		}
		
		return sql.toString();
	}
	
	/**
	 * Sets where clause values to the prepared sql statement
	 * @param statement The statement that is being prepared.
	 * @param startIndex The index where the first value of this clause occurs.
	 * @return The index of the next value insert
	 * @throws SQLException If the operation failed
	 */
	public int setObjectValues(PreparedStatement statement, int startIndex) throws SQLException
	{
		int i = startIndex;
		for (WhereCondition condition : this.conditions)
		{
			i = condition.setObjectValues(statement, i);
		}
		
		return i;
	}
	
	/**
	 * Returns a debug string that mimics the final sql statement created from this where clause
	 * @param targetTable The table this condition is used in
	 * @return A debug sql statement
	 */
	public String getDebubSql(DatabaseTable targetTable)
	{
		if (this.conditions.isEmpty())
			return "";
		
		StringBuilder sql = new StringBuilder(" WHERE");
		
		boolean isFirst = true;
		for (WhereCondition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" AND");
			sql.append(condition.getDebugSql(targetTable));
			isFirst = false;
		}
		
		return sql.toString();
	}
}
