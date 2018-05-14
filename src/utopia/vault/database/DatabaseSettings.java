package utopia.vault.database;

import utopia.flow.structure.Option;
import utopia.vault.generics.BasicSqlDataType;

/**
 * DatabaseSettings is a static collection of variables that are used in 
 * database accessing and handling. Most of the variables are protected so they can only be 
 * used inside this package.
 * @author Mikko Hilpinen
 * @since 17.7.2014
 */
public class DatabaseSettings
{
	// ATTRIBUTES	-----------------------------------------------------
	
	/**
	 * ConnectionTarget is the host of the MariaDB server
	 */
	private static String connectionTarget = "jdbc:mysql://localhost:3306/";
	/**
	 * passWord is the password used to identify the user accessing the database
	 */
	private static Option<String> password = Option.none();
	/**
	 * user is the user used to access the database
	 */
	private static String user = "root";
	/**
	 * The driver used when accessing the database
	 */
	private static Option<String> driver = Option.none();
	
	
	// CONSTRUCTOR	----------------------------------------------------
	
	private DatabaseSettings()
	{
		// Constructor is hidden from other objects
	}
	
	
	// GETTERS & SETTERS	--------------------------------------------
	
	/**
	 * @return The server hosting the database (doesn't include database name)
	 */
	protected static String getConnectionTarget()
	{
		return connectionTarget;
	}
	
	/**
	 * @return The user used for accessing the database
	 */
	protected static String getUser()
	{
		return user;
	}
	
	/**
	 * @return The password used for accessing the database
	 */
	protected static Option<String> getPassword()
	{
		return password;
	}
	
	/**
	 * @return The driver used when accessing the database. None if not initialized or specified
	 */
	protected static Option<String> getDriver()
	{
		return driver;
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
	 * @param newPassword The new password used to identify the user
	 */
	public static void setPassword(String newPassword)
	{
		password = Option.some(newPassword);
	}
	
	/**
	 * Changes the user used to connect to the MariaDB server
	 * @param newUser The name of the user used to connect to the server
	 */
	public static void setUser(String newUser)
	{
		user = newUser;
	}
	
	/**
	 * Changes the driver used when connecting to the database
	 * @param newDriver The new driver that is used
	 */
	public static void setDriver(String newDriver)
	{
		driver = Option.some(newDriver);
	}
	
	
	// OTHER METHODS	--------------------------------
	
	/**
	 * Initializes the databaseSettings to the given values. The sql data types are initialised 
	 * as well.
	 * @param connectionTarget The MariaDB server to be used. Should not include 
	 * the name of the database. None means "jdbc:mysql://localhost:3306/"
	 * @param user The userName used when connecting to the server. None means "root"
	 * @param password The password used when connecting to the server
	 * @param driver The driver used when connecting to the server. Eg. org.gjt.mm.mysql.Driver. 
	 * Null if no driver class is specified (works with normal applications)
	 */
	public static void initialize(Option<String> connectionTarget, Option<String> user, Option<String> password, 
			Option<String> driver)
	{
		connectionTarget.forEach(DatabaseSettings::setConnectionTarget);
		user.forEach(DatabaseSettings::setUser);
		password.forEach(DatabaseSettings::setPassword);
		driver.forEach(DatabaseSettings::setDriver);
		
		// Initialises the sql data types as well
		BasicSqlDataType.initialise();
	}
}
