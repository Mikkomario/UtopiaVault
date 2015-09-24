package vault_database;

/**
 * Name mapping rules can be used in {@link AttributeNameMapping} when there is no direct 
 * mapping from one column name to an attribute name
 * @author Mikko Hilpinen
 * @since 24.9.2015
 */
public interface NameMappingRule
{
	/**
	 * This method is checked before the rule is used for mappings, since some rules may only 
	 * work in certain conditions
	 * @param columnName The name of the column that would be mapped
	 * @return Can the rule be applied to this column name
	 */
	public boolean canMapColumnName(String columnName);
	
	/**
	 * This method is checked when a column name is requested from the mapping, as some rules 
	 * may work backwards as well.
	 * @param attributeName The attribute name that would be retraced back to a column name
	 * @return Is the rule able to retrace back to the original column name from the provided 
	 * attribute name
	 */
	public boolean canRetraceColumnName(String attributeName);
	
	/**
	 * This method maps a column name to an attribute name.
	 * @param columnName The column name that will be mapped
	 * @return The attribute name that will be mapped to the column name
	 */
	public String getAttributeName(String columnName);
	
	/**
	 * This method retraces a column name from an attribute name (if possible).
	 * @param attributeName The name of the attribute
	 * @return The name of the column that would be mapped to this attribute name
	 */
	public String getColumnName(String attributeName);
}
