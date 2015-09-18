package vault_database;

/**
 * DatabaseSettings is a static collection of variables that are used in 
 * database accessing and handling. Most of the variables are protected so they can only be 
 * used inside this package.
 * 
 * @author Mikko Hilpinen
 * @since 17.7.2014
 */
public class DatabaseSettings
{
	// ATTRIBUTES	-----------------------------------------------------
	
	/**
	 * ConnectionTarget is the host of the MariaDB server
	 */
	private static String connectionTarget;
	/**
	 * passWord is the password used to identify the user accessing the database
	 */
	private static String password;
	/**
	 * user is the user used to access the database
	 */
	private static String user;
	
	
	// CONSTRUCTOR	----------------------------------------------------
	
	private DatabaseSettings()
	{
		// Constructor is hidden from other objects
	}
	
	
	// GETTERS & SETTERS	--------------------------------------------
	
	/**
	 * @return The server hosting the database (doesn't include database name)
	 * @throws UninitializedSettingsException If the connection target hasn't been specified
	 */
	protected static String getConnectionTarget() throws UninitializedSettingsException
	{
		if (connectionTarget == null)
			throw new UninitializedSettingsException();
		
		return connectionTarget;
	}
	
	/**
	 * @return The user used for accessing the database
	 * @throws UninitializedSettingsException If the user hasn't been specified
	 */
	protected static String getUser() throws UninitializedSettingsException
	{
		if (user == null)
			throw new UninitializedSettingsException();
		
		return user;
	}
	
	/**
	 * @return The password used for accessing the database
	 * @throws UninitializedSettingsException If the password hasn't been set
	 */
	protected static String getPassword() throws UninitializedSettingsException
	{
		if (password == null)
			throw new UninitializedSettingsException();
		
		return password;
	}
	
	/**
	 * Changes the name of the MariaDB server to be used
	 * @param newTarget The new MariaDB server to be used. Should not include 
	 * the name of the database. For example: "jdbc:mysql://localhost:3306/"
	 */
	public static void setConnectionTarget(String newTarget)
	{
		connectionTarget = newTarget;
	}
	
	/**
	 * Changes the password that is used when accessing the server
	 * 
	 * @param newPassword The new password used to identify the user
	 */
	public static void setPassword(String newPassword)
	{
		password = newPassword;
	}
	
	/**
	 * Changes the user used to connect to the MariaDB server
	 * 
	 * @param newUser The name of the user used to connect to the server
	 */
	public static void setUser(String newUser)
	{
		user = newUser;
	}
	
	
	// OTHER METHODS	--------------------------------
	
	/**
	 * Initializes the databaseSettings to the given values. 
	 * Use the file provided in the data folder to create the multitable database
	 * 
	 * @param connectionTarget The MariaDB server to be used. Should not include 
	 * the name of the database. For example: "jdbc:mysql://localhost:3306/"
	 * @param user The userName used when connecting to the server
	 * @param password The password used when connecting to the server
	 */
	public static void initialize(String connectionTarget, String user, String password)
	{
		setConnectionTarget(connectionTarget);
		setUser(user);
		setPassword(password);
		//initializeMultiTableHandler(maxRowCount);
	}
	
	
	// SUBCLASSES	---------------------------------
	
	/**
	 * This exception is thrown when the settings are used without initialization
	 * 
	 * @author Mikko Hilpinen
	 * @since 23.1.2014
	 */
	public static class UninitializedSettingsException extends Exception
	{
		private static final long serialVersionUID = 1788971592281471774L;

		/**
		 * Creates a new exception
		 */
		private UninitializedSettingsException()
		{
			super("DatabaseSettings haven't been initialized yet");
		}
	}
}
