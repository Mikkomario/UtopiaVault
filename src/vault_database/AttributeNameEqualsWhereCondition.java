package vault_database;

import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DatabaseTable.Column;
import vault_database.EqualsWhereCondition.Operator;

/**
 * This class works like an equals condition, but it can be created using just attribute name 
 * and value pairs. The process becomes unstable if the attribute names don't represent 
 * any columns in the target table, however.
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public class AttributeNameEqualsWhereCondition extends SingleWhereCondition
{
	// ATTRIBUTES	------------------
	
	private String[] attributeNames;
	private Operator operator;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition where the condition checks a table attribute with the 
	 * provided name
	 * @param operator the operator used for checking the attribute value
	 * @param attributeName The name of the table attribute
	 * @param value The value the attribute must have
	 */
	public AttributeNameEqualsWhereCondition(Operator operator, String attributeName, 
			DatabaseValue value)
	{
		super(false, value);
		
		this.attributeNames = new String[1];
		this.attributeNames[0] = attributeName;
		this.operator = operator;
	}
	
	/**
	 * Creates a new where condition that checks two table attributes
	 * @param operator the operator used for checking the attribute values
	 * @param firstAttributeName The name of the first table attribute
	 * @param secondAttributeName The name of the second table attribute
	 */
	public AttributeNameEqualsWhereCondition(Operator operator, String firstAttributeName, 
			String secondAttributeName)
	{
		super(false);
		
		this.operator = operator;
		this.attributeNames = new String[2];
		this.attributeNames[0] = firstAttributeName;
		this.attributeNames[1] = secondAttributeName;
	}
	
	
	// IMPLEMENTED METHODS	-----------------

	@Override
	protected String getSQLWithPlaceholders(DatabaseTable targetTable) throws 
			WhereConditionParseException
	{
		String[] columnNames = new String[this.attributeNames.length];
		int i = 0;
		
		// Finds the used column names and desired data type
		DataType desiredType;
		try
		{
			Column firstColumn = DatabaseTable.findColumnForAttributeName(targetTable, 
					this.attributeNames[0]);
			desiredType = firstColumn.getType();
			columnNames[0] = firstColumn.getName();
			
			// Finds the other column name(s)
			for (i = 1; i < this.attributeNames.length; i++)
			{
				columnNames[i] = targetTable.getAttributeNameMapping().getColumnName(
						this.attributeNames[i]);
			}
		}
		catch (NoColumnForAttributeException e)
		{
			throw new WhereConditionParseException("Attribute name '" + 
					this.attributeNames[i] + "' doesn't represent a column in the table " + 
					targetTable.getTableName(), e);
		}
		
		// Casts the provided values to correct type
		castValuesToDataType(desiredType);
		
		// Checks for null comparisons
		if (!this.operator.acceptsValues(getValues()))
			throw new WhereConditionParseException("Operator '" + this.operator + 
					"' doesn't allow null values");
		
		StringBuilder sql = new StringBuilder(columnNames[0]);
		sql.append(this.operator.toString());
		if (columnNames.length > 1)
			sql.append(columnNames[1]);
		else
			sql.append("?");
		
		return sql.toString();
	}
}
