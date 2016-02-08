package vault_tutorial;

import java.util.Collection;

import vault_database.Database;
import vault_database.DatabaseException;
import vault_database.DatabaseSettings;
import vault_database.DatabaseUnavailableException;

/**
 * This class tests some of the example functions in this package
 * @author Mikko Hilpinen
 * @since 25.11.2015
 */
public class TutorialTest
{
	// CONSTRUCTOR	-------------------
	
	private TutorialTest()
	{
		// The interface is static so no constructor is needed
	}

	
	// MAIN METHOD	-------------------
	
	/**
	 * Starts and runs the test
	 * @param args password, user (optional), connection target (optional)
	 */
	public static void main(String[] args)
	{
		// Checks the command line arguments
		if (args.length < 1)
		{
			System.out.println(
					"Please provide the correct parameters: password, user (default = root), connection string (default = jdbc:mysql://localhost:3306/)");
			System.exit(0);
		}
		
		// Parses the arguments
		String password = args[0];
		String user = null;
		String connection = null;
		
		if (args.length > 1)
			user = args[1];
		if (args.length > 2)
			connection = args[2];
		
		// uses a single connection through the test
		long startMillis = System.currentTimeMillis();
		Database db = new Database();
		try
		{
			// Initialises the database settings
			DatabaseSettings.initialize(connection, user, password, null);
			
			// Inserts some new roles
			ExampleDatabaseQueries.insertRole(0, "client", db);
			ExampleDatabaseQueries.insertRole(1, "admin", db);
			
			// Inserts test users
			ExampleDatabaseQueries.insertUser("Matti", 0, db);
			ExampleDatabaseQueries.insertUser("Matti", 0, db);
			ExampleDatabaseQueries.insertUser("Matti", 1, db);
			ExampleDatabaseQueries.insertUser("Pekka", 0, db);
			ExampleDatabaseQueries.insertUser("Teuvo", 1, db);
			ExampleDatabaseQueries.insertUser("Manu", 1, db);
			
			// Prints the user data
			System.out.println("All users");
			printUsers(ExampleDatabaseQueries.findUsers(db));
			
			// Tests some other queries
			System.out.println("All users with name 'Matti'");
			printUsers(ExampleDatabaseQueries.findUsers("Matti", db));
			System.out.println("All users with name 'Matti' and role 'admin'");
			printUsers(ExampleDatabaseQueries.findUsers("Matti", "admin", db));
			
			System.out.println("Renaming all 'Matti' to 'Manu'");
			ExampleDatabaseQueries.changeUserNames("Matti", "Manu", db);
			System.out.println("Current user names");
			for (String userName : ExampleDatabaseQueries.findUserNames(db))
			{
				System.out.println(userName);
			}
			
			System.out.println("Deleting all admin users named 'Teuvo'");
			ExampleDatabaseQueries.deleteUsers("Teuvo", 1, db);
			System.out.println("Current users");
			printUsers(ExampleDatabaseQueries.findUsers(db));
			
			System.out.println("Deleting the 'admin' role and all admin users");
			ExampleDatabaseQueries.deleteRole("admin", db);
			System.out.println("Current users");
			printUsers(ExampleDatabaseQueries.findUsers(db));
			
			System.out.println("Deleting remaining test data");
			Database.delete(ExampleTables.ROLES, null, null, null, false, db);
			Database.delete(ExampleTables.USERS, null, null, null, false, db);
		}
		catch (DatabaseException e)
		{
			System.err.println("FAILURE");
			System.err.println(e.getDebugMessage());
			e.printStackTrace();
		}
		catch (DatabaseUnavailableException e)
		{
			System.err.println("FAILURE");
			e.printStackTrace();
		}
		finally
		{
			db.closeConnection();
			long duration = System.currentTimeMillis() - startMillis;
			System.out.println(duration + " ms");
		}
	}
	
	
	// OTHER METHODS	------------------
	
	private static void printUsers(Collection<? extends ExampleUserModel> users)
	{
		for (ExampleUserModel user : users)
		{
			// Printing the role from hard-coded values is not the best practice but servers 
			// in the context of this test
			System.out.println(user.getName() + ", role: " + (user.getRoleIndex() == 0 ? 
					"client" : "admin"));
		}
	}
}
