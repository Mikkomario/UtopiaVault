package utopia.java.vault.generics;

/**
 * These exceptions are thrown when necessary references can't be established
 * @author Mikko Hilpinen
 * @since 18.7.2016
 */
public class NoSuchReferenceException extends RuntimeException
{
	private static final long serialVersionUID = 577732472997906722L;

	/**
	 * Creates a new exception
	 * @param from The table that was used
	 * @param to The table that was being joined / referred to
	 */
	public NoSuchReferenceException(Table from, Table to)
	{
		super("There's no reference between " + from + " and " + to);
	}
}