package utopia.vault.generics;

/**
 * These exceptions are thrown when table initialisation (reading column 
 * information, etc) fails. These exceptions are considered to be run time exceptions
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class TableInitialisationException extends RuntimeException
{
	private static final long serialVersionUID = -4211842208696476343L;

	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 */
	public TableInitialisationException(String message)
	{
		super(message);
	}
	
	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 * @param cause The exception that lead to this exception
	 */
	public TableInitialisationException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
