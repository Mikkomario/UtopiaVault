package utopia.vault.generics;

import java.util.Collection;

import utopia.flow.structure.Graph;

/**
 * This static entity keeps track of column references between different tables
 * @author Mikko Hilpinen
 * @since 13.7.2016
 */
public class TableReferences
{
	// ATTRIBUTES	-----------------
	
	private static TableReferences instance = null;
	
	private Graph<Table, Reference[]> references = new Graph<>();
	
	
	// CONSTRUCTOR	-----------------
	
	private TableReferences()
	{
		// Hidden constructor
	}
	
	/**
	 * @return The static references instance
	 */
	public static TableReferences getInstance()
	{
		if (instance == null)
			instance = new TableReferences();
		return instance;
	}
	
	
	// INTERFACES	-----------------
	
	/**
	 * A reference reader can be used for initialising reference data
	 * @author Mikko Hilpinen
	 * @since 13.7.2016
	 */
	public static interface ReferenceReader
	{
		/**
		 * Reads the references from a certain table to another table. This method will be 
		 * called the first time a reference is requested between the tables.
		 * @param from The table that contains the referring column
		 * @param to The table that contains the referenced column
		 * @return The references from the first table to the second table
		 */
		public Reference[] getReferencesBetween(Table from, Table to);
	}

	
	// NESTED CLASSES	-------------
	
	/**
	 * A reference is a lonk between two tables where one column refers to the value of another. 
	 * Reference instances are immutable.
	 * @author Mikko Hilpinen
	 * @since 13.7.2016
	 */
	public static class Reference
	{
		// ATTRIBUTES	-------------
		
		private final Column from, to;
		
		
		// CONSTRUCTOR	-------------
		
		/**
		 * Creates a new reference
		 * @param from The column that refers to another column
		 * @param to The column referred to
		 */
		public Reference(Column from, Column to)
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
}
