package vault_database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A where clause is a set of where conditions that must be met. The clause can be used in 
 * sql operations. The clause is immutable once created.
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public class CombinedWhereCondition extends WhereCondition
{	
	// ATTRIBUTES	-----------------
	
	private List<WhereCondition> conditions;
	private CombinationOperator operator;
	
	
	// CONSTRUCTOR	-----------------

	private CombinedWhereCondition(CombinationOperator operator, WhereCondition firstCondition, 
			WhereCondition secondCondition, WhereCondition... additionalConditions)
	{
		this.operator = operator;
		this.conditions = new ArrayList<>();
		this.conditions.add(firstCondition);
		this.conditions.add(secondCondition);
		
		for (WhereCondition condition : additionalConditions)
		{
			this.conditions.add(condition);
		}
	}
	
	/**
	 * Combines 2 or more conditions to a new condition that is true when all of the conditions 
	 * are true.
	 * @param firstCondition The first condition that must be true
	 * @param secondCondition The second condition that must be true
	 * @param additionalConditions Other conditions that must be true
	 * @return The conditions combined with AND
	 */
	public static CombinedWhereCondition createANDCombination(WhereCondition firstCondition, 
			WhereCondition secondCondition, WhereCondition... additionalConditions)
	{
		return new CombinedWhereCondition(CombinationOperator.AND, firstCondition, 
				secondCondition, additionalConditions);
	}
	
	/**
	 * Combines 2 or more conditions to a new condition that is true when any of the conditions 
	 * is true.
	 * @param firstCondition The first condition
	 * @param secondCondition The second condition
	 * @param additionalConditions Other conditions
	 * @return The conditions combined with OR
	 */
	public static CombinedWhereCondition createORConbination(WhereCondition firstCondition, 
			WhereCondition secondCondition, WhereCondition... additionalConditions)
	{
		return new CombinedWhereCondition(CombinationOperator.OR, firstCondition, 
				secondCondition, additionalConditions);
	}
	
	/**
	 * Combines 2 conditions to a new condition that is true when the two conditions have 
	 * different logical results
	 * @param firstCondition The first condition
	 * @param secondCondition The second condition
	 * @return The conditions combined with XOR
	 */
	public static CombinedWhereCondition createXORCombination(WhereCondition firstCondition, 
			WhereCondition secondCondition)
	{
		return new CombinedWhereCondition(CombinationOperator.XOR, firstCondition, 
				secondCondition);
	}
	
	
	// OTHER METHODS	--------------
	
	@Override
	public String toSql(DatabaseTable targetTable) throws WhereConditionParseException
	{
		StringBuilder sql = new StringBuilder("(");
		
		boolean isFirst = true;
		for (WhereCondition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" " + this.operator.toSql() + " ");
			sql.append(condition.toSql(targetTable));
			isFirst = false;
		}
		
		sql.append(")");
		return sql.toString();
	}
	
	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex) throws SQLException
	{
		int i = startIndex;
		for (WhereCondition condition : this.conditions)
		{
			i = condition.setObjectValues(statement, i);
		}
		
		return i;
	}

	@Override
	public String getDebugSql(DatabaseTable targetTable)
	{
		// TODO: WET WET
		StringBuilder sql = new StringBuilder("(");
		
		boolean isFirst = true;
		for (WhereCondition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" " + this.operator.toSql() + " ");
			sql.append(condition.getDebugSql(targetTable));
			isFirst = false;
		}
		
		sql.append(")");
		return sql.toString();
	}
	
	
	// ENUMERATIONS	------------------
	
	/**
	 * These are the operators the combinations can be made with
	 * @author Mikko Hilpinen
	 * @since 2.10.2015
	 */
	private static enum CombinationOperator
	{
		/**
		 * True if all of the conditions are true
		 */
		AND,
		/**
		 * True if any of the conditions is true
		 */
		OR,
		/**
		 * True if two conditions have different results
		 */
		XOR;
		
		
		// OTHER METHODS	------------
		
		private String toSql()
		{
			return toString();
		}
	}
}
