package utopia.vault.database;

/**
 * These exceptions are thrown when sql statments / clauses can't be parsed / used
 * @author Mikko Hilpinen
 * @since 2.10.2015
 */
public class StatementParseException extends Exception
{
	private static final long serialVersionUID = -7800912556657335734L;
	
	// CONSTRUCTOR	--------------------

	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 * @param source The source of the exception
	 */
	public StatementParseException(String message, Throwable source)
	{
		super(message, source);
	}
	
	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 */
	public StatementParseException(String message)
	{
		super(message);
	}
}