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
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new equals condition where the two provided values must be equal
	 * @param dataType The data type of the two objects
	 * @param inverted Should the condition be not equals instead
	 * @param first The first value
	 * @param second The second value
	 */
	public EqualsWhereCondition(int dataType, boolean inverted, Object first, Object second)
	{
		super(dataType, inverted, first, second);
		
		this.usedColumns = new String[0];
	}
	
	/**
	 * Creates a new equals condition where the attribute column must have the provided value
	 * @param inverted Should the condition be not equals instead
	 * @param attribute The attribute that describes a column value pair
	 */
	public EqualsWhereCondition(boolean inverted, Attribute attribute)
	{
		super(attribute.getDescription().getType(), inverted, attribute.getValue());
		
		this.usedColumns = new String[1];
		this.usedColumns[0] = attribute.getDescription().getColumnName();
	}
	
	/**
	 * Creates a new equals condition where the column must have the provided value
	 * @param inverted Should the condition be not equals instead
	 * @param columnName The name of the table column
	 * @param columnValue The value the table column must have
	 */
	public EqualsWhereCondition(boolean inverted, String columnName, Object columnValue)
	{
		super(-1, inverted, columnValue);
		
		this.usedColumns = new String[1];
		this.usedColumns[0] = columnName;
	}
	
	/**
	 * Creates a new equals condition where the two column values must be the same
	 * @param inverted Should the condition be not equals instead
	 * @param firstColumnName The name of the first column
	 * @param secondColumnName The name of the second column
	 */
	public EqualsWhereCondition(boolean inverted, String firstColumnName, String secondColumnName)
	{
		super(-1, inverted);
		
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
		return new EqualsWhereCondition(false, index);
	}
	
	
	// IMPLEMENTED METHODS	-----------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable) throws WhereConditionParseException
	{
		// Updates the data type if necessary
		if (getDataType() < 0)
		{
			Column column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
					this.usedColumns[0]);
			if (column == null)
			{
				column = DatabaseTable.findColumnWithName(targetTable.getColumnInfo(), 
						this.usedColumns[1]);
				if (column == null)
					throw new WhereConditionParseException("Table " + 
							targetTable.getTableName() + " contains neither column " + 
							this.usedColumns[0] + " or " + this.usedColumns[2]);
			}
			updateDataType(column.getType());
		}
		
		StringBuilder sql = new StringBuilder();
		if (this.usedColumns.length == 0)
			sql.append("?");
		else
			sql.append(this.usedColumns[0]);
		
		sql.append(" <=> ");
		
		if (this.usedColumns.length > 1)
			sql.append(this.usedColumns[1]);
		else
			sql.append("?");
		
		return sql.toString();
	}
}
