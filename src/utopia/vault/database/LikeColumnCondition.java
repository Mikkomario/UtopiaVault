package utopia.vault.database;

import utopia.flow.generics.Value;
import utopia.vault.generics.Column;

/**
 * This condition searches for rows which the value is like. For example, one may find columns 
 * which contain the start of string "Searched". This would include columns with values "S", 
 * "Sea", "Search" and "Searched", for example.
 * @author Mikko Hilpinen
 * @since 21.10.2016
 */
public class LikeColumnCondition extends SingleCondition
{
	// ATTRIBUTES	--------------
	
	/**
	 * The string representing any character sequence in a search
	 */
	public static final String ANY = LikeCondition.ANY;
	/**
	 * The string representing any single character in a search
	 */
	public static final String ANY_SINGLE = LikeCondition.ANY_SINGLE;
	
	private Column column;
	private String startFilter = null;
	private String endFilter = null;
	
	
	// CONSTRUCTOR	-------------
	
	/**
	 * Creates a new condition that matches the value against a column with patterns
	 * @param value The value the column patterns are matched against
	 * @param beforeColumn The pattern before the column value (optional)
	 * @param column The column matched against the value
	 * @param afterColumn The pattern after the column value (optional)
	 */
	public LikeColumnCondition(String value, String beforeColumn, Column column, String afterColumn)
	{
		super(Value.String(value));
		
		this.startFilter = beforeColumn;
		this.endFilter = afterColumn;
		this.column = column;
	}
	
	/**
	 * Creates a condition that finds rows where the provided value starts with the colum's value
	 * @param value The matched value
	 * @param column The column that is searched
	 * @return A condition that finds rows where the provided value starts with the column's value
	 */
	public static LikeColumnCondition valueStartsWithColumn(String value, Column column)
	{
		return new LikeColumnCondition(value, null, column, ANY);
	}
	
	/**
	 * Creates a condition that finds rows where the provided value ends with the colum's value
	 * @param value The matched value
	 * @param column The column that is searched
	 * @return A condition that finds rows where the provided value ends with the column's value
	 */
	public static LikeColumnCondition valueEndsWithColumn(String value, Column column)
	{
		return new LikeColumnCondition(value, ANY, column, null);
	}
	
	/**
	 * Creates a condition that finds rows where the provided value contains the colum's value
	 * @param value The matched value
	 * @param column The column that is searched
	 * @return A condition that finds rows where the provided value contains the column's value
	 */
	public static LikeColumnCondition valueContainsColumn(String value, Column column)
	{
		return new LikeColumnCondition(value, ANY, column, ANY);
	}
	
	
	// IMPLEMENTED METHODS	----

	@Override
	protected String getDebugSqlWithNoParsing()
	{
		return getSql();
	}

	@Override
	public String toSql() throws StatementParseException
	{
		// Checks that no null values were provided
		if (this.column == null)
			throw new StatementParseException("Column can't be null");
		for (Value value : getValues())
		{
			if (value.isNull())
				throw new StatementParseException("Matched value can't be null");
		}
		
		return getSql();
	}
	
	
	// OTHER METHODS	----------
	
	private String getSql()
	{
		boolean hasStartFilter = this.startFilter != null && !this.startFilter.isEmpty();
		boolean hasEndFilter = this.endFilter != null && !this.endFilter.isEmpty();
		
		StringBuilder sql = new StringBuilder("? LIKE ");
		if (!hasStartFilter && !hasEndFilter)
			sql.append(sql.append(this.column.getColumnNameWithTable()));
		else
		{
			sql.append("CONCAT(");
			
			if (hasStartFilter)
			{
				sql.append(this.startFilter);
				sql.append(", ");
			}
			
			if (this.column == null)
				sql.append("NULL");
			else
				sql.append(this.column.getColumnNameWithTable());
			
			if (hasEndFilter)
			{
				sql.append(", ");
				sql.append(this.endFilter);
			}
			
			sql.append(")");
		}
		
		return sql.toString();
	}
}
