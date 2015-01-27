package vault_database;

/**
 * These exceptions are thrown when the given table is unsuited for its purpose
 * 
 * @author Mikko Hilpinen
 * @since 27.1.2015
 */
public class InvalidTableTypeException extends Exception
{
	private static final long serialVersionUID = 1473068369833129084L;

	/**
	 * Creates a new exception
	 * @param message The message sent with the exception
	 */
	public InvalidTableTypeException(String message)
	{
		super(message);
	}
}
