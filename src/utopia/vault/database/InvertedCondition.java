package utopia.vault.database;

import utopia.flow.generics.Value;

/**
 * This condition wraps and inverts another. The logical value of this condition will always 
 * be inverted to that of the original condition.
 * @author Mikko Hilpinen
 * @since 18.7.2016
 */
public class InvertedCondition extends Condition
{
	// ATTRIBUTES	------------------
	
	private Condition condition;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new condition that is inverted to the provided condition
	 * @param condition a condition
	 */
	public InvertedCondition(Condition condition)
	{
		this.condition = condition;
	}
	
	
	// IMPLEMENTED METHODS	---------

	@Override
	public Value[] getValues()
	{
		return this.condition.getValues();
	}

	@Override
	public String toSql() throws StatementParseException
	{
		return "NOT " + this.condition.toSql();
	}

	@Override
	public String getDebugSql()
	{
		return "NOT " + this.condition.getDebugSql();
	}
}
