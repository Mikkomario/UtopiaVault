package utopia.vault.generics;

/**
 * A reference reader can be used for initialising table reference data
 * @author Mikko Hilpinen
 * @since 13.7.2016
 */
public interface TableReferenceReader
{
	/**
	 * Reads the references from a certain table to another table. This method will be 
	 * called the first time a reference is requested between the tables.
	 * @param from The table that contains the referring column
	 * @param to The table that contains the referenced column
	 * @return The references from the first table to the second table
	 */
	public TableReference[] getReferencesBetween(Table from, Table to);
}