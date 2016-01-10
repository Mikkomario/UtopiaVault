package vault_tutorial;

import java.util.Collection;

import vault_database_old.DatabaseAccessor;
import vault_database_old.DatabaseException;
import vault_database_old.DatabaseSettings;
import vault_database_old.DatabaseUnavailableException;
import vault_database_old.AttributeNameMapping.MappingException;
import vault_tutorial.ExampleUserModel.ExampleRoleModel;

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
		
		try
		{
			// Initialises the database settings
			DatabaseSettings.initialize(connection, user, password, null);
			
			// Inserts some new roles
			ExampleRoleModel role1 = new ExampleRoleModel(0, "client");
			ExampleRoleModel role2 = new ExampleRoleModel(1, "admin");
			
			DatabaseAccessor.insertObjectToDatabaseIfNotExists(role1);
			DatabaseAccessor.insertObjectToDatabaseIfNotExists(role2);
			
			// Inserts test users
			ExampleDatabaseQueries.insertUser("Matti", 0);
			ExampleDatabaseQueries.insertUser("Matti", 0);
			ExampleDatabaseQueries.insertUser("Matti", 1);
			ExampleDatabaseQueries.insertUser("Pekka", 0);
			ExampleDatabaseQueries.insertUser("Teuvo", 1);
			ExampleDatabaseQueries.insertUser("Manu", 1);
			
			// Prints the user data
			System.out.println("All users");
			printUsers(ExampleDatabaseQueries.findUsers());
			
			// Tests some other queries
			System.out.println("All users with name 'Matti'");
			printUsers(ExampleDatabaseQueries.findUsers("Matti"));
			System.out.println("All users with name 'Matti' and role 'admin'");
			printUsers(ExampleDatabaseQueries.findUsers("Matti", "admin"));
			
			System.out.println("Renaming all 'Matti' to 'Manu'");
			ExampleDatabaseQueries.changeUserNames("Matti", "Manu");
			System.out.println("Current user names");
			for (String userName : ExampleDatabaseQueries.findUserNames())
			{
				System.out.println(userName);
			}
			
			System.out.println("Deleting all admin users named 'Teuvo'");
			ExampleDatabaseQueries.deleteUsers("Teuvo", 1);
			System.out.println("Current users");
			printUsers(ExampleDatabaseQueries.findUsers());
			
			System.out.println("Deleting the 'admin' role and all admin users");
			ExampleDatabaseQueries.deleteRole("admin");
			System.out.println("Current users");
			printUsers(ExampleDatabaseQueries.findUsers());
			
			System.out.println("Deleting remaining test data");
			DatabaseAccessor.delete(ExampleTable.USERS, null);
			DatabaseAccessor.delete(ExampleTable.ROLES, null);
		}
		catch (DatabaseException e)
		{
			System.err.println("FAILURE");
			System.err.println(e.getDebugMessage());
			e.printStackTrace();
		}
		catch (DatabaseUnavailableException | MappingException e)
		{
			System.err.println("FAILURE");
			e.printStackTrace();
		}
	}
	
	
	// OTHER METHODS	------------------
	
	private static void printUsers(Collection<? extends ExampleUserModel> users)
	{
		for (ExampleUserModel user : users)
		{
			// Printing the role from hard-coded values is not the best practice but servers 
			// in the context of this test
			System.out.println(user.getName() + ", role: " + (user.getRoleId() == 0 ? 
					"client" : "admin"));
		}
	}
}
