package utopia.vault.generics;

/**
 * This rule drops all underlines '_' from a column name in order to form a variable name. 
 * Camel case is applied instead.
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class UnderlinesToCamelCaseRule implements NameMappingRule
{
	// TODO: TEST
	
	// ATTRIBUTES	-------------------
	
	private static UnderlinesToCamelCaseRule instance = null;
	
	
	// CONSTRUCTOR	-------------------
	
	private UnderlinesToCamelCaseRule()
	{
		// Constructor hidden
	}
	
	/**
	 * @return The static rule instance
	 */
	public static UnderlinesToCamelCaseRule getInstance()
	{
		if (instance == null)
			instance = new UnderlinesToCamelCaseRule();
		return instance;
	}
	
	
	// IMPLEMENTED METHODS	----------------

	@Override
	public boolean canMapColumnName(String columnName)
	{
		return true;
	}

	@Override
	public boolean canRetraceColumnName(String variableName)
	{
		return true;
	}

	@Override
	public String getVariableName(String columnName)
	{
		StringBuilder parsed = new StringBuilder();
		int cursor = 0;
		
		// Moves the cursor until all of the string has been read
		while (cursor < columnName.length())
		{
			// Finds the next underline
			int nextUnderline = cursor + columnName.substring(cursor).indexOf('_');
			
			// If there are no more underlines, adds the rest of the string
			if (nextUnderline < cursor)
			{
				parsed.append(columnName.substring(cursor));
				break;
			}
			else
			{
				// Parses until the next underline, the next character is set to upper case
				parsed.append(columnName.substring(cursor, nextUnderline));
				if (columnName.length() > nextUnderline + 1)
				{
					parsed.append(Character.toUpperCase(columnName.charAt(nextUnderline + 1)));
					cursor = nextUnderline + 2;
				}
				else
					break;
			}
		}
		
		return parsed.toString();
	}

	@Override
	public String getColumnName(String variableName)
	{
		StringBuilder s = new StringBuilder();
		for (char c : variableName.toCharArray())
		{
			char lower = Character.toLowerCase(c);
			if (c == lower)
				s.append(c);
			else
			{
				s.append('_');
				s.append(lower);
			}
		}
		
		return s.toString();
	}
}
