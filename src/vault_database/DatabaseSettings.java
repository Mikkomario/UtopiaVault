package vault_database;

import java.sql.SQLException;

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
	
	/**
	 * The MultiTableHandler is used for handling table indexing and is needed by the 
	 * classes that insert data into tables
	 */
	private static MultiTableHandler tableHandler;
	
	
	// CONSTRUCTOR	----------------------------------------------------
	
	private DatabaseSettings()
	{
		// Constructor is hidden from other objects
	}
	
	
	// GETTERS & SETTERS	--------------------------------------------
	
	/**
	 * @return The multiTableHandler used when accessing the databases
	 */
	public static MultiTableHandler getTableHandler()
	{
		return tableHandler;
	}
	
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
	
	/**
	 * Initializes the multiTableHandler if it hasn't been initialized yet. 
	 * Use the file provided in the data folder to create the multitable database
	 * 
	 * @param maxRowCount How many rows can a single table hold
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void initializeMultiTableHandler(int maxRowCount) throws 
			DatabaseUnavailableException, SQLException
	{
		if (tableHandler == null)
			tableHandler = new MultiTableHandler(maxRowCount);
	}
	
	/**
	 * Initializes the multiTableHandler if it hasn't been initialized yet. 
	 * Use the file provided in the data folder to create the multitable database
	 * 
	 * @param maxRowCount How many rows can a single table hold
	 * @param databaseName The name of the database that holds the multitable data
	 * @param tableName The name of the table that holds the multitable data
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void initializeMultiTableHandler(int maxRowCount, String databaseName, 
			String tableName) throws DatabaseUnavailableException, SQLException
	{
		if (tableHandler == null)
			tableHandler = new MultiTableHandler(maxRowCount, databaseName, tableName);
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
	 * @param maxRowCount How many rows can a single table hold
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void initialize(String connectionTarget, String user, String password, 
			int maxRowCount) throws DatabaseUnavailableException, SQLException
	{
		setConnectionTarget(connectionTarget);
		setUser(user);
		setPassword(password);
		initializeMultiTableHandler(maxRowCount);
	}
	
	/**
	 * Initializes the databaseSettings to the given values. 
	 * Use the file provided in the data folder to create the multitable database
	 * 
	 * @param connectionTarget The MariaDB server to be used. Should not include 
	 * the name of the database. For example: "jdbc:mysql://localhost:3306/"
	 * @param user The userName used when connecting to the server
	 * @param password The password used when connecting to the server
	 * @param maxRowCount How many rows can a single table hold
	 * @param databaseName The name of the database that holds the multitable data
	 * @param tableName The name of the table that holds the multitable data
	 * @throws SQLException If the given table is malformed or missing
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void initialize(String connectionTarget, String user, String password, 
			int maxRowCount, String databaseName, 
			String tableName) throws DatabaseUnavailableException, SQLException
	{
		setConnectionTarget(connectionTarget);
		setUser(user);
		setPassword(password);
		initializeMultiTableHandler(maxRowCount, databaseName, tableName);
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
