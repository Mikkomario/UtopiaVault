package vault_recording;

import java.util.HashMap;
import java.util.Map;

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
	 * @return A column name mapped to this attribute. Null if no column name is mapped to 
	 * the attribute name.
	 */
	public String getColumnName(String attributeName)
	{
		for (String columnName : this.names.keySet())
		{
			if (this.names.get(columnName).equalsIgnoreCase(attributeName))
				return columnName;
		}
		return null;
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
}
