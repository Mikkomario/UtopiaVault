package vault_generics;

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
	 * @throws DatabaseTableInitialisationException If the initialisation failed
	 */
	public Collection<? extends Column> generateColumns(Table table) 
			throws DatabaseTableInitialisationException;
	
	
	// NESTED CLASSES	----------------
	
	/**
	 * These exceptions are thrown when database table initialisation (reading column 
	 * information, etc) fails
	 * @author Mikko Hilpinen
	 * @since 9.1.2016
	 */
	public static class DatabaseTableInitialisationException extends RuntimeException
	{
		private static final long serialVersionUID = -4211842208696476343L;

		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 */
		public DatabaseTableInitialisationException(String message)
		{
			super(message);
		}
		
		/**
		 * Creates a new exception
		 * @param message The message sent along with the exception
		 * @param cause The exception that lead to this exception
		 */
		public DatabaseTableInitialisationException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}
}
