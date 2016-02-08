package utopia.vault.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A combined condition is a set of conditions that must be met. The clause can be used in 
 * sql operations. The condition is immutable once created.
 * @author Mikko Hilpinen
 * @since 1.10.2015
 */
public class CombinedCondition extends Condition
{	
	// ATTRIBUTES	-----------------
	
	private List<Condition> conditions;
	private CombinationOperator operator;
	
	
	// CONSTRUCTOR	-----------------

	private CombinedCondition(CombinationOperator operator, 
			Condition... conditions) throws ConditionParseException
	{
		this.operator = operator;
		
		// Checks that enough conditions were provided
		if (conditions.length < 2 || (this.operator == CombinationOperator.XOR && conditions.length > 2))
			throw new ConditionParseException("Operator " + operator + 
					" doesn't work with " + conditions.length + " operands");
		
		this.conditions = new ArrayList<>();
		for (Condition condition : conditions)
		{
			this.conditions.add(condition);
		}
	}
	
	private CombinedCondition(CombinationOperator operator, 
			Condition firstCondition, Condition secondCondition, 
			Condition... additionalConditions)
	{
		this.operator = operator;
		this.conditions = new ArrayList<>();
		this.conditions.add(firstCondition);
		this.conditions.add(secondCondition);
		
		for (Condition condition : additionalConditions)
		{
			this.conditions.add(condition);
		}
	}
	
	/**
	 * Combines a bunch of conditions together
	 * @param operator The operator that separates the conditions
	 * @param conditions The conditions that are combined (0 or more)
	 * @return Null if no conditions were provided, a single condition if only one 
	 * condition was provided, a combined condition if multiple conditions were provided
	 * @throws ConditionParseException If {@link CombinationOperator#XOR} was used with 
	 * more than 2 operands
	 */
	public static Condition combineConditions(CombinationOperator operator, 
			Condition... conditions) throws ConditionParseException
	{
		if (conditions.length == 0)
			return null;
		else if (conditions.length == 1)
			return conditions[0];
		else
			return new CombinedCondition(operator, conditions);
	}
	
	/**
	 * Combines 2 or more conditions to a new condition that is true when all of the conditions 
	 * are true.
	 * @param firstCondition The first condition that must be true
	 * @param secondCondition The second condition that must be true
	 * @param additionalConditions Other conditions that must be true
	 * @return The conditions combined with AND
	 */
	public static CombinedCondition createANDCombination(Condition firstCondition, 
			Condition secondCondition, Condition... additionalConditions)
	{
		return new CombinedCondition(CombinationOperator.AND, 
				firstCondition, secondCondition, additionalConditions);
	}
	
	/**
	 * Combines 2 or more conditions to a new condition that is true when any of the conditions 
	 * is true.
	 * @param firstCondition The first condition
	 * @param secondCondition The second condition
	 * @param additionalConditions Other conditions
	 * @return The conditions combined with OR
	 */
	public static CombinedCondition createORConbination(Condition firstCondition, 
			Condition secondCondition, Condition... additionalConditions)
	{
		return new CombinedCondition(CombinationOperator.OR, 
				firstCondition, secondCondition, additionalConditions);
	}
	
	/**
	 * Combines 2 conditions to a new condition that is true when the two conditions have 
	 * different logical results
	 * @param firstCondition The first condition
	 * @param secondCondition The second condition
	 * @return The conditions combined with XOR
	 */
	public static CombinedCondition createXORCombination(Condition firstCondition, 
			Condition secondCondition)
	{
		return new CombinedCondition(CombinationOperator.XOR, 
				firstCondition, secondCondition, new Condition[0]);
	}
	
	
	// OTHER METHODS	--------------
	
	@Override
	public String toSql() throws ConditionParseException
	{
		StringBuilder sql = new StringBuilder("(");
		
		boolean isFirst = true;
		for (Condition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" " + this.operator.toSql() + " ");
			sql.append(condition.toSql());
			isFirst = false;
		}
		
		sql.append(")");
		return sql.toString();
	}
	
	@Override
	public int setObjectValues(PreparedStatement statement, int startIndex) throws 
			SQLException, ConditionParseException
	{
		int i = startIndex;
		for (Condition condition : this.conditions)
		{
			i = condition.setObjectValues(statement, i);
		}
		
		return i;
	}

	@Override
	public String getDebugSql()
	{
		// TODO: WET WET
		StringBuilder sql = new StringBuilder("(");
		
		boolean isFirst = true;
		for (Condition condition : this.conditions)
		{
			if (!isFirst)
				sql.append(" " + this.operator.toSql() + " ");
			sql.append(condition.getDebugSql());
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
	public static enum CombinationOperator
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
