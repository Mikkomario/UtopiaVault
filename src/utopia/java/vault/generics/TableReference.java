package utopia.java.vault.generics;

/**
 * A reference is a link between two tables where one column refers to the value of another. 
 * Reference instances are immutable.
 * @author Mikko Hilpinen
 * @since 13.7.2016
 */
public class TableReference
{
	// ATTRIBUTES	-------------
	
	private final Column from, to;
	
	
	// CONSTRUCTOR	-------------
	
	/**
	 * Creates a new reference
	 * @param from The column that refers to another column
	 * @param to The column referred to
	 */
	public TableReference(Column from, Column to)
	{
		this.from = from;
		this.to = to;
	}
	
	
	// ACCESSORS	-------------
	
	/**
	 * @return The column that refers to another column
	 */
	public Column getReferencingColumn()
	{
		return this.from;
	}
	
	/**
	 * @return The column referred to
	 */
	public Column getReferencedColumn()
	{
		return this.to;
	}
}