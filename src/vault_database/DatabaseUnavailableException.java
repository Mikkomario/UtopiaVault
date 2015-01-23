package vault_database;

/**
 * These exceptions are thrown when the desired database can't be accessed
 * 
 * @author Mikko Hilpinen
 * @since 23.1.2014
 */
public class DatabaseUnavailableException extends Exception
{
	private static final long serialVersionUID = 4084772706721749210L;

	/**
	 * Creates a new exception
	 * @param message The message sent with the exception
	 */
	public DatabaseUnavailableException(String message)
	{
		super(message);
	}

	/**
	 * Creates a new exception
	 * @param cause The cause of the exception
	 */
	public DatabaseUnavailableException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * Creates a new exception
	 * @param message The message sent with the exception
	 * @param cause The cause of the exception
	 */
	public DatabaseUnavailableException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
