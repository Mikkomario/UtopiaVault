package vault_recording;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import vault_database.DatabaseTable.ColumnInfo;

/**
 * This map connects column names to attribute names. All names are case-insensitive. 
 * "STRING" would be considered equal with "string"
 * @author Mikko Hilpinen
 * @since 18.9.2015
 */
public class AttributeNameMapping
{
	// ATTRIBUTES	-------------------
	
	private Map<String, String> names; // column name, attribute name
	
	
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new empty mapping. Attribute names must be added separately.
	 */
	public AttributeNameMapping()
	{
		this.names = new HashMap<>();
	}

	
	// OTHER METHODS	---------------
	
	/**
	 * Finds the attribute name mapped to the column name
	 * @param columnName The column name
	 * @return An attribute name mapped to the column name. If there is no mapping, the column 
	 * name is returned.
	 */
	public String getAttributeName(String columnName)
	{
		String lowerCase = columnName.toLowerCase();
		if (this.names.containsKey(lowerCase))
			return this.names.get(lowerCase);
		return lowerCase;
	}
	
	/**
	 * Finds a column name mapped to this attribute name
	 * @param attributeName The attribute name
	 * @param nullIfNotMapped Should null be returned in case where there is no mapping for 
	 * the given attribute name
	 * @return If there is a mapping, returns a column name mapped to this attribute. Null if 
	 * nullIfNotMapped is true and 
	 * no column name is mapped to the attribute name. The name of the attribute otherwise.
	 */
	public String getColumnName(String attributeName, boolean nullIfNotMapped)
	{
		for (String columnName : this.names.keySet())
		{
			if (this.names.get(columnName).equalsIgnoreCase(attributeName))
				return columnName;
		}
		if (nullIfNotMapped)
			return null;
		else
			return attributeName;
	}
	
	/**
	 * Creates a new name association, possible previous association will be replaced
	 * @param columnName The name of the column
	 * @param attributeName The attribute name mapped to the column name
	 */
	public void addMapping(String columnName, String attributeName)
	{
		this.names.put(columnName.toLowerCase(), attributeName.toLowerCase());
	}
	
	/**
	 * Checks if there is a mapping for the provided column name
	 * @param columnName The column name
	 * @return Is there a mapping for the provided column name
	 */
	public boolean containsMappingForColumn(String columnName)
	{
		return this.names.containsKey(columnName.toLowerCase());
	}
	
	/**
	 * Checks if any column is mapped to the provided attribute name
	 * @param attributeName The attribute name some columns may be mapped to
	 * @return Is there a column name mapped to the provided attribute name
	 */
	public boolean containsMappingForAttribute(String attributeName)
	{
		return this.names.keySet().contains(attributeName.toLowerCase());
	}
	
	/**
	 * Searches through the collection for a column that is mapped to the given attribute name
	 * @param columns The columns
	 * @param attributeName The name of the attribute that represents a column
	 * @return The column the mapped to the attribute name or null if no such column exists in 
	 * the collection
	 */
	public ColumnInfo findColumnForAttribute(Collection<? extends ColumnInfo> columns, 
			String attributeName)
	{
		for (ColumnInfo column : columns)
		{
			if (getAttributeName(column.getName()).equalsIgnoreCase(attributeName))
				return column;
		}
		
		return null;
	}
}
