package vault_database;

import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DatabaseTable.Column;

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
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new where condition where the table attribute with the provided name must 
	 * have the provided value
	 * @param inverted Should the condition be not equals instead
	 * @param attributeName The name of the table attribute
	 * @param value The value the attribute must have
	 */
	public AttributeNameEqualsWhereCondition(boolean inverted, String attributeName, Object value)
	{
		super(-1, inverted, value);
		
		this.attributeNames = new String[1];
		this.attributeNames[0] = attributeName;
	}
	
	/**
	 * Creates a new where condition where the two table attributes must have equal values
	 * @param inverted Should the condition be not equals instead
	 * @param firstAttributeName The name of the first table attribute
	 * @param secondAttributeName The name of the second table attribute
	 */
	public AttributeNameEqualsWhereCondition(boolean inverted, String firstAttributeName, 
			String secondAttributeName)
	{
		super(-1, inverted);
		
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
		
		// Finds the data type
		try
		{
			Column firstColumn = DatabaseTable.findColumnForAttributeName(targetTable, 
					this.attributeNames[0]);
			updateDataType(firstColumn.getType());
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
		
		StringBuilder sql = new StringBuilder(columnNames[0]);
		sql.append(" <=> ");
		if (columnNames.length > 1)
			sql.append(columnNames[1]);
		else
			sql.append("?");
		
		return sql.toString();
	}
}
