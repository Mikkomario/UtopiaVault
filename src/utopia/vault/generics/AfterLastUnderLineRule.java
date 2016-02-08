package utopia.vault.generics;


/**
 * This rule cuts away all characters before the last underline in the column name. The rule 
 * is a singular object.
 * @author Mikko Hilpinen
 * @since 24.9.2015
 * @see #getInstance()
 */
public class AfterLastUnderLineRule implements NameMappingRule
{
	// ATTRIBUTES	-----------------------------
	
	private static AfterLastUnderLineRule instance;
	
	
	// CONSTRUCTOR	-----------------------------
	
	private AfterLastUnderLineRule()
	{
		// Hidden constructor
	}
	
	
	// IMPLEMENTED METHODS	---------------------

	@Override
	public boolean canMapColumnName(String columnName)
	{
		return true;
	}

	@Override
	public boolean canRetraceColumnName(String attributeName)
	{
		return false;
	}

	@Override
	public String getVariableName(String columnName)
	{
		int cutAt = columnName.lastIndexOf('_');
		return columnName.substring(cutAt + 1);
	}

	@Override
	public String getColumnName(String attributeName)
	{
		// Can't retrace lost characters
		return null;
	}
	
	
	// ACCESSORS	--------------------------
	
	/**
	 * @return An instance of this class. The instance will be shared among the requesters.
	 */
	public static AfterLastUnderLineRule getInstance()
	{
		if (instance == null)
			instance = new AfterLastUnderLineRule();
		
		return instance;
	}
}
