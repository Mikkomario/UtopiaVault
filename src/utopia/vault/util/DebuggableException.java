package utopia.vault.util;

/**
 * This exception contains a debug message
 * @author Mikko Hilpinen
 * @since 4.11.2015
 */
public class DebuggableException extends Exception
{
	// ATTRIBUTES	--------------
	
	private static final long serialVersionUID = 8561625636300430698L;

	private String debugMessage;
	
	
	// CONSTRUCTOR	--------------

	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 * @param debugMessage The debug message
	 */
	public DebuggableException(String message, String debugMessage)
	{
		super(message);
		
		this.debugMessage = debugMessage;
	}
	
	/**
	 * Creates a new exception
	 * @param debugMessage The debug message
	 * @param cause The cause of this exception
	 */
	public DebuggableException(String debugMessage, Throwable cause)
	{
		super(cause);
		
		this.debugMessage = debugMessage;
	}

	/**
	 * Creates a new exception
	 * @param message The message sent along with the exception
	 * @param debugMessage The debug message
	 * @param cause The cause of the exception
	 */
	public DebuggableException(String message, String debugMessage, Throwable cause)
	{
		super(message, cause);
		
		this.debugMessage = debugMessage;
	}
	
	
	// ACCESSORS	-------------------
	
	/**
	 * @return The debug message of this exception
	 */
	public String getDebugMessage()
	{
		return this.debugMessage;
	}
	
	/**
	 * Changes the debug message used
	 * @param debugMessage The new debug message
	 */
	protected void setDebugMessage(String debugMessage)
	{
		this.debugMessage = debugMessage;
	}
}
