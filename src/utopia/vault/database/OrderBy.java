package utopia.vault.database;

import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.Pair;
import utopia.vault.generics.Column;

/**
 * An orderBy is used for sorting the results
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class OrderBy
{
	// ATTRIBUTES	----------------
	
	// Column, ascending
	private ImmutableList<Pair<Column, Boolean>> columns;
	
	
	// CONSTRUCTOR	----------------
	
	/**
	 * Creates a new order
	 * @param columns The columns used in the order, plus whether the order shuold be ascending per column
	 */
	public OrderBy(ImmutableList<Pair<Column, Boolean>> columns)
	{
		this.columns = columns;
	}
	
	/**
	 * Creates a new order by statement
	 * @param columns The column(s) the results are ordered by
	 * @param ascending Should the ordering be ascending (true), or descending (false), 
	 * a separate value can be given for each column. Ascending order is used by default. 
	 * (optional)
	 */
	public OrderBy(Column[] columns, Boolean[] ascending)
	{
		ImmutableList<Boolean> asc = ImmutableList.of(ascending);
		if (asc.size() < columns.length)
			asc = asc.plus(ImmutableList.filledWith(columns.length - asc.size(), true));
		
		this.columns = ImmutableList.of(columns).mergedWith(asc);
	}

	/**
	 * Creates a new order by statement where either ascending order or descending order is used 
	 * for all the sorting conditions
	 * @param ascending Should the order be ascending (true) or descending (false)
	 * @param columns The columns that are used in the sorting
	 */
	public OrderBy(boolean ascending, Column... columns)
	{
		this.columns = ImmutableList.of(columns).mergedWith(ImmutableList.filledWith(columns.length, ascending));
	}
	
	/**
	 * Creates a new order by statement where a single column is used in the sorting
	 * @param column The column that is used for the sorting
	 * @param ascending Should the sorting be ascending (true) or descending (false)
	 */
	public OrderBy(Column column, boolean ascending)
	{
		this.columns = ImmutableList.withValue(new Pair<>(column, ascending));
	}
	
	/**
	 * Creates a new order by statement where multiple columns are used, and rows are returned 
	 * in ascending order
	 * @param columns The columns used in the sorting
	 */
	public OrderBy(Column... columns)
	{
		this.columns = ImmutableList.of(columns).mergedWith(ImmutableList.filledWith(columns.length, true));
	}
	
	
	// IMPLEMENTED METHODS	-------
	
	@Override
	public String toString()
	{
		return toSql();
	}
	
	
	// OTHER METHODS	-----------
	
	/**
	 * @return The order by as an sql clause like " ORDER BY column1 ASC, column2 DESC"
	 */
	public String toSql()
	{
		if (this.columns.isEmpty())
			return "";
		
		StringBuilder sql = new StringBuilder(" ORDER BY ");
		sql.append(this.columns.map(OrderBy::columnAscToString).reduce((total, part) -> total + ", " + part));
		
		return sql.toString();
	}
	
	private static String columnAscToString(Pair<Column, Boolean> data)
	{
		return data.getFirst().getColumnNameWithTable() + (data.getSecond() ? " ASC" : " DESC");
	}
}
