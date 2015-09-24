package vault_database;

/**
 * This rule drops n characters from the beginning of a column name to form an attribute name
 * @author Mikko Hilpinen
 * @since 24.9.2015
 */
public class DropFromBeginningRule implements NameMappingRule
{
	// ATTRIBUTES	---------------------
	
	private int dropAmount;
	
	
	// CONSTRUCTOR	---------------------
	
	/**
	 * Creates a new rule
	 * @param dropAmount How many characters are dropped from each column name
	 */
	public DropFromBeginningRule(int dropAmount)
	{
		this.dropAmount = dropAmount;
	}

	/**
	 * Can only map column names that are long enough
	 */
	@Override
	public boolean canMapColumnName(String columnName)
	{
		return columnName.length() > this.dropAmount;
	}

	@Override
	public boolean canRetraceColumnName(String attributeName)
	{
		return false;
	}

	@Override
	public String getAttributeName(String columnName)
	{
		return columnName.substring(this.dropAmount);
	}

	@Override
	public String getColumnName(String attributeName)
	{
		// Can't retrace lost characters
		return null;
	}
}
