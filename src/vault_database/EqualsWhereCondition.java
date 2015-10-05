package vault_database;

import vault_database.DatabaseTable.Column;
import vault_recording.DatabaseWritable;

/**
 * This condition is true when two values equal or are both null
 * @author Mikko Hilpinen
 * @since 2.10.2010
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
			if (column == null)
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
