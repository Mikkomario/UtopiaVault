package utopia.vault.generics;

/**
 * Name mapping rules can be used in {@link VariableNameMapping} when there is no direct 
 * mapping from one column name to an variable name
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
	 * @param variableName The variable name that would be retraced back to a column name
	 * @return Is the rule able to retrace back to the original column name from the provided 
	 * variable name
	 */
	public boolean canRetraceColumnName(String variableName);
	
	/**
	 * This method maps a column name to an variable name.
	 * @param columnName The column name that will be mapped
	 * @return The variable name that will be mapped to the column name
	 */
	public String getVariableName(String columnName);
	
	/**
	 * This method retraces a column name from an variable name (if possible).
	 * @param variableName The name of the variable
	 * @return The name of the column that would be mapped to this variable name
	 */
	public String getColumnName(String variableName);
}
