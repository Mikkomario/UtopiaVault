package utopia.vault.database;

import utopia.vault.generics.Column;

/**
 * An orderBy is used for sorting the results
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class OrderBy
{
	// ATTRIBUTES	----------------
	
	private Column[] columns;
	private boolean[] ascs;
	
	
	// CONSTRUCTOR	----------------
	
	/**
	 * Creates a new order by statement
	 * @param columns The column(s) the results are ordered by
	 * @param ascending Should the ordering be ascending (true), or descending (false), 
	 * a separate value can be given for each column. Ascending order is used by default. 
	 * (optional)
	 */
	public OrderBy(Column[] columns, boolean[] ascending)
	{
		this.columns = columns;
		this.ascs = ascending;
		
		if (this.ascs == null)
			this.ascs = new boolean[0];
	}

	/**
	 * Creates a new order by statement where either ascending order or descending order is used 
	 * for all the sorting conditions
	 * @param ascending Should the order be ascending (true) or descending (false)
	 * @param columns The columns that are used in the sorting
	 */
	public OrderBy(boolean ascending, Column... columns)
	{
		this.columns = columns;
		this.ascs = new boolean[] {ascending};
	}
	
	/**
	 * Creates a new order by statement where a single column is used in the sorting
	 * @param column The column that is used for the sorting
	 * @param ascending Should the sorting be ascending (true) or descending (false)
	 */
	public OrderBy(Column column, boolean ascending)
	{
		this.columns = new Column[] {column};
		this.ascs = new boolean[] {ascending};
	}
	
	/**
	 * Creates a new order by statement where multiple columns are used, and rows are returned 
	 * in ascending order
	 * @param columns The columns used in the sorting
	 */
	public OrderBy(Column... columns)
	{
		this.columns = columns;
		this.ascs = new boolean[0];
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
		if (this.columns.length == 0)
			return "";
		
		StringBuilder sql = new StringBuilder(" ORDER BY ");
		for (int i = 0; i < this.columns.length; i++)
		{
			if (i != 0)
				sql.append(", ");
			sql.append(this.columns[i].getColumnName());
			if (this.ascs.length > i)
			{
				if (this.ascs[i])
					sql.append(" ASC");
				else
					sql.append(" DESC");
			}
		}
		
		return sql.toString();
	}
}
