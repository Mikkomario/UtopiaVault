package utopia.vault.generics;

import java.util.Collection;

/**
 * Classes implementing this interface are able to initialise column data in a database 
 * table when requested to do so.
 * @author Mikko Hilpinen
 * @since 10.1.2016
 */
public interface ColumnInitialiser
{
	/**
	 * This method is called when a database table needs to get its columns initialised
	 * @param table The table that is requesting the column data. Please note that requesting 
	 * the table's column data from the initialiser will result in a stack overflow.
	 * @return The columns for the table
	 * @throws TableInitialisationException If the initialisation failed
	 */
	public Collection<? extends Column> generateColumns(Table table) 
			throws TableInitialisationException;
}
