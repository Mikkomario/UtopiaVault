package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.vault.generics.Column;

/**
 * Like condition is used for checking whether the value of a column matches a pattern
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class LikeCondition extends SingleCondition
{
	// ATTRIBUTES	----------------
	
	/**
	 * An expression that represents any single character in the condition
	 */
	public static final String ANY_SINGLE = "_";
	/**
	 * An expression that represents any character sequence in the condition
	 */
	public static final String ANY = "%";
	
	private Column column;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a new condition
	 * @param column The column that should have a matching value
	 * @param like a string the column value should match. Use '%' to represent any number of 
	 * any characters and '_' to represent any single character.
	 * @param inverted Should the condition be NOT LIKE instead
	 */
	public LikeCondition(Column column, String like, boolean inverted)
	{
		super(inverted, Value.String(like));
		this.column = column;
	}

	/**
	 * Creates a new condition
	 * @param column The column that should have a matching value
	 * @param like a string the column value should match. Use '%' to represent any number of 
	 * any characters and '_' to represent any single character.
	 */
	public LikeCondition(Column column, String like)
	{
		super(false, Value.String(like));
		this.column = column;
	}
	
	/**
	 * Creates a condition that is selects rows whose column value starts with the 
	 * provided string
	 * @param column a column
	 * @param s The string the columns value must start with
	 * @return the resulting condition
	 */
	public static LikeCondition startsWith(Column column, String s)
	{
		return new LikeCondition(column, s + ANY);
	}
	
	/**
	 * Creates a condition that selects rows whose column value ends with the 
	 * provided string
	 * @param column a column
	 * @param s The string the columns value must end with
	 * @return the resulting condition
	 */
	public static LikeCondition endsWith(Column column, String s)
	{
		return new LikeCondition(column, ANY + s);
	}
	
	/**
	 * Creates a condition that selects rows whose column value contains the 
	 * provided string
	 * @param column a column
	 * @param s The string the columns value must contain
	 * @return the resulting condition
	 */
	public static LikeCondition contains(Column column, String s)
	{
		return new LikeCondition(column, ANY + s + ANY);
	}
	
	
	/**
	 * Creates a condition that selects rows whose column value is of certain length
	 * @param column a column
	 * @param lenght The length the column value must have
	 * @return the resulting condition
	 */
	public static LikeCondition hasLength(Column column, int lenght)
	{
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < lenght; i++)
		{
			s.append(ANY_SINGLE);
		}
		
		return new LikeCondition(column, s.toString());
	}
	
	
	// IMPLEMENTED METHODS	---------

	@Override
	protected String getSQLWithPlaceholders() throws ConditionParseException
	{
		// Checks that the provided values are acceptable
		if (this.column == null)
			throw new ConditionParseException("Compared column can't be null");
		for (Value value : getValues())
		{
			if (value == null || value.isNull())
				throw new ConditionParseException("Compared value can't be null");
		}
		
		return getSql();
	}

	@Override
	protected String getDebugSqlWithNoParsing()
	{
		if (this.column == null)
			return "null LIKE ?";
		return getSql();
	}

	
	// OTHER METHODS	-------------
	
	private String getSql()
	{
		StringBuilder sql = new StringBuilder();
		sql.append(this.column.getColumnNameWithTable());
		sql.append(" LIKE ?");
		
		return sql.toString();
	}
}
