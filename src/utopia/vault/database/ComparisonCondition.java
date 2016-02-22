package utopia.vault.database;

import java.util.Collection;

import utopia.flow.generics.Value;
import utopia.vault.database.CombinedCondition.CombinationOperator;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.TableModel;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * This condition compares a column with a value or two columns with each other
 * @author Mikko Hilpinen
 * @since 17.1.2016
 */
public class ComparisonCondition extends SingleCondition
{
	// ATTRIBUTES	------------------
	
	private Column[] columns;
	private Operator operator;
	
	
	// CONSTRUCTROR	------------------
	
	/**
	 * Creates a new comparison condition for two database columns
	 * @param firstColumn The column on the left side of the comparison
	 * @param operator The operator used for comparing the column values
	 * @param secondColumn The column on the right side of the comparison
	 */
	public ComparisonCondition(Column firstColumn, Operator operator, Column secondColumn)
	{
		this.columns = new Column[] {firstColumn, secondColumn};
		this.operator = operator;
	}
	
	/**
	 * Creates a new condition for two database columns. The condition is true if both columns 
	 * have equal values
	 * @param firstColumn The column on the left side of the comparison
	 * @param secondColumn The column on the right side of the comparison
	 */
	public ComparisonCondition(Column firstColumn, Column secondColumn)
	{
		this.columns = new Column[] {firstColumn, secondColumn};
		this.operator = Operator.EQUALS;
	}
	
	/**
	 * Creates a new comparison condition for a column's value
	 * @param column The column which is compared
	 * @param operator The operator that is used in the comparison
	 * @param value The value the column's value is compared with
	 */
	public ComparisonCondition(Column column, Operator operator, Value value)
	{
		super(value);
		this.columns = new Column[] {column};
		this.operator = operator;
	}
	
	/**
	 * Creates a new condition for a column's value. The column must have the provided value 
	 * in order for the condition to return true
	 * @param column The column which is compared
	 * @param value The value the column's value is compared with
	 */
	public ComparisonCondition(Column column, Value value)
	{
		super(value);
		this.columns = new Column[] {column};
		this.operator = Operator.EQUALS;
	}
	
	/**
	 * Creates a new comparison condition for a column's value
	 * @param variable The variable that is compared to the database equivalent
	 * @param operator The operator used in the comparison
	 */
	public ComparisonCondition(ColumnVariable variable, Operator operator)
	{
		super(variable.getValue());
		this.columns = new Column[] {variable.getColumn()};
		this.operator = operator;
	}
	
	/**
	 * Creates a new condition that checks whether the provided variable is equal to the 
	 * database state (where the associated column has the same value as the provided variable)
	 * @param variable a variable
	 */
	public ComparisonCondition(ColumnVariable variable)
	{
		super(variable.getValue());
		this.columns = new Column[] {variable.getColumn()};
		this.operator = Operator.EQUALS;
	}
	
	/**
	 * Creates a new condition that compares primary keys with the model's equivalent
	 * @param model A model
	 * @param operator The operator used in the comparison
	 * @return A condition
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 */
	public static ComparisonCondition createIndexCondition(TableModel model, 
			Operator operator)
	{
		return new ComparisonCondition(model.getIndexAttribute(), operator);
	}
	
	/**
	 * Creates a new condition that returns true only for the row that has the same primary 
	 * key as the model's index / equivalent
	 * @param model A model
	 * @return A condition
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 */
	public static ComparisonCondition createIndexEqualsCondition(TableModel model) 
			throws NoSuchColumnException
	{
		return createIndexCondition(model, Operator.EQUALS);
	}
	
	/**
	 * Creates a new condition that checks multiple variable values with 
	 * {@link Operator#EQUALS} comparison.
	 * @param variables The variables that are checked
	 * @param combinationOperator The operator used for combining the conditions
	 * @return A combined condition based on the variables and the provided operator
	 * @throws ConditionParseException If XOR was used with more than 2 variables
	 */
	public static Condition createVariableSetEqualsCondition(
			Collection<? extends ColumnVariable> variables, 
			CombinationOperator combinationOperator) throws ConditionParseException
	{
		if (variables.isEmpty())
			return null;
		
		Condition[] conditions = new Condition[variables.size()];
		int i = 0;
		for (ColumnVariable variable : variables)
		{
			conditions[i] = new ComparisonCondition(variable);
			i ++;
		}
		
		return CombinedCondition.combineConditions(combinationOperator, conditions);
	}
	
	/**
	 * Creates a new condition that checks multiple variable values with 
	 * {@link Operator#EQUALS} comparison. Each of the variables must match the row's 
	 * equivalents for the row to be selected.
	 * @param variables The variables that are checked
	 * @return A combined condition based on the variables
	 */
	public static Condition createVariableSetEqualsCondition(
			Collection<? extends ColumnVariable> variables)
	{
		try
		{
			return createVariableSetEqualsCondition(variables, CombinationOperator.AND);
		}
		catch (ConditionParseException e)
		{
			// This exception is only thrown when using XOR, this time AND is used so 
			// this block shouldn't be reached
			throw new RuntimeException("Where condition combination failed even when using AND", 
					e);
		}
	}
	
	/**
	 * Creates a condition that only selects rows that have state identical to the model's 
	 * declared attributes
	 * @param model a model
	 * @return a condition
	 */
	public static Condition createModelEqualsCondition(TableModel model)
	{
		return createVariableSetEqualsCondition(model.getAttributes());
	}
	
	
	// IMPLEMENTED METHODS	---------

	@Override
	protected String getSQLWithPlaceholders() throws ConditionParseException
	{
		// Checks if some columns were null
		for (int i = 0; i < this.columns.length; i++)
		{
			Column column = this.columns[i];
			if (column == null)
				throw new ConditionParseException("Unknown / null column used at index " + i);
		}
		
		// Specifies the desired data type
		specifyValueDataType(this.columns[0].getSqlType());
		// Makes sure the values are accepted by the operator
		if (!this.operator.acceptsValues(getValues()))
			throw new ConditionParseException("Operator" + this.operator + 
					"doesn't accept null values");
		
		StringBuilder sql = new StringBuilder();
		appendSql(sql);
		
		return sql.toString();
	}

	@Override
	protected String getDebugSqlWithNoParsing()
	{
		StringBuilder sql = new StringBuilder();
		appendSql(sql);
		return sql.toString();
	}
	
	
	// OTHER METHODS	---------------
	
	private void appendSql(StringBuilder sql)
	{
		// TODO: If table names don't work in all conditions, add a special attribute to 
		// determine whether they should be added or not
		appendColumn(sql, this.columns[0]);
		
		sql.append(this.operator);
		
		if (this.columns.length < 2)
			sql.append("?");
		else
			appendColumn(sql, this.columns[1]);
	}
	
	private static void appendColumn(StringBuilder sql, Column column)
	{
		if (column == null)
			sql.append("null");
		else
			sql.append(column.getColumnNameWithTable());
	}

	
	// ENUMS	-----------------------
	
	/**
	 * These are the different operators used for comparing values / columns with each other 
	 * in sql
	 * @author Mikko Hilpinen
	 * @since 5.10.2015
	 */
	public static enum Operator
	{
		/**
		 * True if both values are equal or null, false otherwise
		 */
		EQUALS(" <=> "), 
		/**
		 * True if the values are different, false otherwise. Fails on null.
		 */
		NOT_EQUALS(" <> "),
		/**
		 * True if both values are equal, or if the first value is smaller, false otherwise. Fails on null.
		 */
		EQUALS_OR_SMALLER(" <= "), 
		/**
		 * True if the first value is smaller, false otherwise. Fails on null.
		 */
		SMALLER(" < "), 
		/**
		 * True if both values are equal, or if the first value is larger, false otherwise. Fails on null.
		 */
		EQUALS_OR_LARGER(" >= "), 
		/**
		 * True if the first value is larger, false otherwise. Fails on null.
		 */
		LARGER(" > ");
		
		
		// ATTRIBUTES	-------------
		
		private final String sql;
		
		
		// CONSTRUCTOR	-------------
		
		private Operator(String sql)
		{
			this.sql = sql;
		}
		
		
		// IMPLEMENTED METHODS	-----
		
		@Override
		public String toString()
		{
			return this.sql;
		}
		
		
		// OTHER METHODS	---------
		
		/**
		 * @return Does the operator allow null values. Only equals allows nulls.
		 */
		public boolean allowsNull()
		{
			return this == EQUALS;
		}
		
		/**
		 * Checks if the operator is able to handle the provided value set (checks null values)
		 * @param values The values used with this operator
		 * @return Are the values valid for this operator
		 */
		public boolean acceptsValues(Value[] values)
		{
			if (allowsNull())
				return true;
			
			// Checks for null comparisons
			for (Value value : values)
			{
				if (value.isNull())
					return false;
			}
			
			return true;
		}
	}
}