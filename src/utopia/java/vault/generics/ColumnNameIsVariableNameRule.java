package utopia.java.vault.generics;


/**
 * This name mapping rule simply maps a column name to an attribute name as is. The rule 
 * can be applied in all situations. The class is a singular.
 * @author Mikko Hilpinen
 * @since 24.9.2015
 * @see #getInstance()
 */
public class ColumnNameIsVariableNameRule implements NameMappingRule
{
	// ATTRIBUTES	----------------------------
	
	private static ColumnNameIsVariableNameRule instance;
	
	
	// CONSTRUCTOR	----------------------------
	
	private ColumnNameIsVariableNameRule()
	{
		// Hidden constructor
	}
	
	
	// IMPLEMENTED METHODS	--------------------

	@Override
	public boolean canMapColumnName(String columnName)
	{
		return true;
	}

	@Override
	public boolean canRetraceColumnName(String attributeName)
	{
		return true;
	}

	@Override
	public String getVariableName(String columnName)
	{
		return columnName;
	}

	@Override
	public String getColumnName(String attributeName)
	{
		return attributeName;
	}
	
	
	// ACCESSORS	---------------------------
	
	/**
	 * @return A instance of this rule. The instance will be shared among the objects.
	 */
	public static ColumnNameIsVariableNameRule getInstance()
	{
		if (instance == null)
			instance = new ColumnNameIsVariableNameRule();
		
		return instance;
	}
}
