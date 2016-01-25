package vault_database_old;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import vault_database.ComparisonCondition;
import vault_database_old.CombinedWhereCondition.CombinationOperator;
import vault_database_old.DatabaseTable.Column;
import vault_recording.DatabaseWritable;

/**
 * This condition is true when two values equal or are both null
 * @author Mikko Hilpinen
 * @since 2.10.2010
 * @deprecated Replaced with {@link ComparisonCondition}
 */
public class EqualsWhereCondition extends SingleWhereCondition
{
	// ATTRIBUTES	-------------------
	
	private String[] usedColumns;
	private Operator operator;
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new equals condition where the two provided values must be equal
	 * @param operator The operator used for checking the value(s)
	 * @param first The first value
	 * @param second The second value
	 */
	public EqualsWhereCondition(Operator operator, DatabaseValue first, DatabaseValue second)
	{
		super(false, first, second);
		
		this.usedColumns = new String[0];
		this.operator = operator;
	}
	
	/**
	 * Creates a new equals condition where the attribute column must have the provided value
	 * @param operator The operator used for checking the value(s)
	 * @param attribute The attribute that describes a column value pair
	 */
	public EqualsWhereCondition(Operator operator, Attribute attribute)
	{
		super(false, attribute.getValue());
		
		this.operator = operator;
		this.usedColumns = new String[1];
		this.usedColumns[0] = attribute.getDescription().getColumnName();
	}
	
	/**
	 * Creates a new equals condition where the column must have the provided value
	 * @param operator The operator used for checking the value(s)
	 * @param columnName The name of the table column
	 * @param columnValue The value the table column must have
	 */
	public EqualsWhereCondition(Operator operator, String columnName, DatabaseValue columnValue)
	{
		super(false, columnValue);
		
		this.operator = operator;
		this.usedColumns = new String[1];
		this.usedColumns[0] = columnName;
	}
	
	/**
	 * Creates a new equals condition where the two column values must be the same
	 * @param operator The operator used for checking the value(s)
	 * @param firstColumnName The name of the first column
	 * @param secondColumnName The name of the second column
	 */
	public EqualsWhereCondition(Operator operator, String firstColumnName, String secondColumnName)
	{
		super(false);
		
		this.operator = operator;
		this.usedColumns = new String[2];
		this.usedColumns[0] = firstColumnName;
		this.usedColumns[1] = secondColumnName;
	}
	
	/**
	 * Creates a where condition that is true only for the models index
	 * @param model The model
	 * @return A where condition that only allows the row with the model's primary index
	 * @throws IndexAttributeRequiredException If the model doesn't have an index attribute
	 */
	public static EqualsWhereCondition createWhereIndexCondition(DatabaseWritable model) 
			throws IndexAttributeRequiredException
	{
		Attribute index = DatabaseWritable.getIndexAttribute(model);
		if (index == null)
			throw new IndexAttributeRequiredException(model, 
					"Can't search for object index for object that doesn't have an index");
		return new EqualsWhereCondition(Operator.EQUALS, index);
	}
	
	/**
	 * Creates a new where condition that is true when the provided operator is true for 
	 * each attribute in the provided collection, basically chaining multiple attribute conditions 
	 * with AND.
	 * @param operator The operator used for the attribute conditions (usually equals)
	 * @param attributes a set of attributes that form the condition
	 * @param noNullConditions Should null value attributes be skipped when creating the 
	 * condition
	 * @return A where condition. Null if the condition allows all rows (empty or null 
	 * attributes collection or all attributes were null and noNullConditions was true)
	 */
	public static WhereCondition createWhereModelAttributesCondition(Operator operator, 
			Collection<? extends Attribute> attributes, boolean noNullConditions)
	{
		return createWhereModelAttributesCondition(operator, attributes, noNullConditions, 
				CombinationOperator.AND);
	}
	
	/**
	 * Creates a new where condition that is true when the provided operator is true for 
	 * each attribute in the provided collection, basically chaining multiple attribute conditions 
	 * with a combination operator.
	 * @param operator The operator used for the attribute conditions (usually equals)
	 * @param attributes a set of attributes that form the condition
	 * @param noNullConditions Should null value attributes be skipped when creating the 
	 * condition
	 * @param combinationOperator The operator that is used for combining the conditions 
	 * (default = AND)
	 * @return A where condition. Null if the condition allows all rows (empty or null 
	 * attributes collection or all attributes were null and noNullConditions was true)
	 */
	public static WhereCondition createWhereModelAttributesCondition(Operator operator, 
			Collection<? extends Attribute> attributes, boolean noNullConditions, 
			CombinationOperator combinationOperator)
	{
		if (attributes == null)
			return null;
		
		List<Attribute> conditionAttributes = new ArrayList<>();
		if (noNullConditions)
		{
			for (Attribute attribute : attributes)
			{
				if (!attribute.isNull())
					conditionAttributes.add(attribute);
			}
		}
		else
			conditionAttributes.addAll(attributes);
		
		WhereCondition[] conditions = new WhereCondition[conditionAttributes.size()];
		for (int i = 0; i < conditions.length; i++)
		{
			conditions[i] = new EqualsWhereCondition(operator, conditionAttributes.get(i));
		}
		
		try
		{
			return CombinedWhereCondition.combineConditions(combinationOperator, conditions);
		}
		catch (WhereConditionParseException e)
		{
			// XOR not used so this won't be reached
			return null;
		}
	}
	
	
	// IMPLEMENTED METHODS	-----------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable) throws 
			WhereConditionParseException
	{
		// Updates the value data types to match column data types (only if both columns and values are used)
		if (this.usedColumns.length > 0 && getValues().length > 0)
		{
			// Finds out the desired data type from a column
			Column column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
					this.usedColumns[0]);
			if (column == null && this.usedColumns.length > 1)
			{
				column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
						this.usedColumns[1]);
				/*
				if (column == null)
					throw new WhereConditionParseException("Table " + 
							targetTable.getTableName() + " contains neither column " + 
							this.usedColumns[0] + " or " + this.usedColumns[2]);
							*/
			}
			
			// Casts the values to the correct data type (if possible)
			if (column != null)
				castValuesToDataType(column.getType());
		}
		
		// Checks for null comparisons
		if (!this.operator.acceptsValues(getValues()))
			throw new WhereConditionParseException("Operator '" + this.operator + 
					"' doesn't allow null values");
		
		StringBuilder sql = new StringBuilder();
		if (this.usedColumns.length == 0)
			sql.append("?");
		else
			sql.append(this.usedColumns[0]);
		
		sql.append(this.operator.toString());
		
		if (this.usedColumns.length > 1)
			sql.append(this.usedColumns[1]);
		else
			sql.append("?");
		
		return sql.toString();
	}
	
	@Override
	protected String getDebugSqlWithNoParsing(DatabaseTable targetTable)
	{
		StringBuilder s = new StringBuilder();
		
		if (this.usedColumns.length == 0)
			s.append(this.usedColumns[0]);
		else
			s.append("?");
		
		s.append(this.operator.toString());
		
		if (this.usedColumns.length > 1)
			s.append(this.usedColumns[1]);
		else
			s.append("?");
		
		return s.toString();
	}
	
	
	
	// ENUMS	-----------------------
	
	/**
	 * These are the different operators used for checking equality
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
		 * Checks if the operator is able to handle the provided value set
		 * @param values The values used with this operator
		 * @return Are the values valid for this operator
		 */
		public boolean acceptsValues(DatabaseValue[] values)
		{
			if (allowsNull())
				return true;
			
			// Checks for null comparisons
			for (DatabaseValue value : values)
			{
				if (value.isNull())
					return false;
			}
			
			return true;
		}
	}
}
