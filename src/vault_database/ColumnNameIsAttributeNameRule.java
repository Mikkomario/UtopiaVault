package vault_database;

/**
 * This name mapping rule simply maps a column name to an attribute name as is. The rule 
 * can be applied in all situations. The class is a singular.
 * @author Mikko Hilpinen
 * @since 24.9.2015
 * @see #getInstance()
 */
public class ColumnNameIsAttributeNameRule implements NameMappingRule
{
	// ATTRIBUTES	----------------------------
	
	private static ColumnNameIsAttributeNameRule instance;
	
	
	// CONSTRUCTOR	----------------------------
	
	private ColumnNameIsAttributeNameRule()
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
	public String getAttributeName(String columnName)
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
	public static ColumnNameIsAttributeNameRule getInstance()
	{
		if (instance == null)
			instance = new ColumnNameIsAttributeNameRule();
		
		return instance;
	}
}
