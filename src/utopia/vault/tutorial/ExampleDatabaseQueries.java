package utopia.vault.tutorial;

import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.flow.util.Option;
import utopia.vault.database.Condition;
import utopia.vault.database.Database;
import utopia.vault.database.DatabaseException;
import utopia.vault.database.DatabaseUnavailableException;
import utopia.vault.database.Join;
import utopia.vault.database.OrderBy;
import utopia.vault.database.Selection;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * This is a static collection of example methods that perform different database queries 
 * based on the other example resources.
 * @author Mikko Hilpinen
 * @since 24.11.2015
 */
public class ExampleDatabaseQueries
{
	// CONSTRUCTOR	-------------------
	
	private ExampleDatabaseQueries()
	{
		// The methods are static so constructor isn't needed
	}

	
	// OTHER METHODS	---------------
	
	/**
	 * Finds a single user's data based on the provided index
	 * @param userId The user's index
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return The user with the provided index or null if no such user exists
	 * @throws DatabaseException if the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static ExampleUserModel findUser(int userId, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// The model is first created as empty
		ExampleUserModel user = new ExampleUserModel();
		// The method finds the first row with the given index and reads the data into the 
		// user model. If the operation was a success, the model is returned
		if (Database.readModelAttributes(user.toDatabaseModel(), Value.Integer(userId), connection))
			return user;
		// It is often better to return null than an empty model
		return null;
	}
	
	/**
	 * This method finds and parses all user data stored in the database
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return All users stored in the database
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static ImmutableList<ExampleUserModel> findUsers(Database connection) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// One can construct models with partial selects, but select all 
		// is often the best option
		ImmutableList<ImmutableList<ColumnVariable>> results = Database.select(Selection.ALL, ExampleTables.USERS, 
				Option.none(), Option.none(), Option.none(), connection);
		
		// The users are parsed in a separate method to avoid repeating code
		return parseUsers(results);
	}
	
	/**
	 * This method finds and parses all users stored in the database that have username that 
	 * starts with the provided string
	 * @param userName a string the user names should start with
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return All users in the database that have a name that starts with the provided string
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed
	 */
	public static ImmutableList<ExampleUserModel> findUsers(String userName, Database connection) 
			throws DatabaseException, DatabaseUnavailableException
	{
		// Like previously, all data is selected. This time a where condition is used, though.
		ImmutableList<ImmutableList<ColumnVariable>> results = Database.select(Selection.ALL, ExampleTables.USERS, 
				Option.some(ExampleConditions.createUserNameStartsWithCondition(userName)), Option.none(), 
				Option.none(), connection);
		
		// The users are parsed just like previously
		return parseUsers(results);
	}
	
	/**
	 * This method finds and parses all users that have the provided name and a role with 
	 * the provided name
	 * @param userName The required user name
	 * @param roleName The required role name
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return All users which have the required attributes
	 * @throws DatabaseException if the operation fails
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static ImmutableList<ExampleUserModel> findUsers(String userName, String roleName, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		// This time only the user attributes are selected, as opposed to all, which 
		// would select the role attributes as well (since the roles are joined)
		Selection selection = new Selection(ExampleTables.USERS);
		
		// A join condition is used. The condition needs to be wrapped into an array since 
		// the method expects one or more conditions
		// We can use a where condition that was specified in the ExampleWhereConditions
		ImmutableList<ImmutableList<ColumnVariable>> results = Database.select(
				selection, ExampleTables.USERS, Join.createReferenceJoins(ExampleTables.USERS, 
				ExampleTables.ROLES), Option.some(ExampleConditions.createUserNameAndRoleNameCondition(
				userName, roleName)), Option.none(), Option.none(), connection);
		
		// The users are parsed the same way as always
		return parseUsers(results);
	}
	
	/**
	 * This method finds all the user names that exist in the database
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A set containing each user name in the database
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static ImmutableList<String> findUserNames(Database connection) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// This time only the user name is selected
		Selection selection = new Selection(ExampleTables.USERS, "name");
		// No where clause is used this time. The results come in same format as always
		// The user names are ordered in alphabetical order (desc)
		OrderBy order = new OrderBy(selection.getColumns().head(), false);
		ImmutableList<ImmutableList<ColumnVariable>> results = Database.select(selection, ExampleTables.USERS, 
				Option.none(), Option.none(), Option.some(order), connection);
		
		// Each list in the result represents a row in the database
		// Each row then contains a single user name
		// The user name is the first and only attribute that is returned for each row.
		// That's why we can simply call head().
		// The attribute value must also be parsed to string before it is added to the set
		return results.map(row -> row.head().getValue().toString());
	}
	
	/**
	 * This method creates and inserts a user to the database
	 * @param userName The name of the user
	 * @param userRoleId The identifier of the user's role
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return The user that was inserted
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static ExampleUserModel insertUser(String userName, int userRoleId, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		// The user is simply created as a new user model
		ExampleUserModel user = new ExampleUserModel();
		user.setName(userName);
		user.setRoleIndex(userRoleId);
		
		// The model can be easily inserted to the database. And since it doesn't have a 
		// specified index, it can't already exist in the database
		// The model also receives its index attribute (id) when this method is called
		Database.insert(user.toDatabaseModel(), connection);
		
		return user;
	}
	
	/**
	 * Creates a new role and inserts it to the database. If the role already exists in the 
	 * database, it is updated instead
	 * @param roleIndex The row index of the role
	 * @param roleName The name of the role
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return The role that was created
	 * @throws DatabaseException If the query failed
	 * @throws NoSuchColumnException If the role table doesn't have a primary column
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static ExampleRoleModel insertRole(int roleIndex, String roleName, 
			Database connection) throws 
			DatabaseException, NoSuchColumnException, DatabaseUnavailableException
	{
		ExampleRoleModel role = new ExampleRoleModel();
		role.setIndex(Value.Integer(roleIndex));
		role.setName(roleName);
		
		Database.insertOrUpdate(role, false, connection);
		
		return role;
	}
	
	/**
	 * Replaces a user name with another in the database
	 * @param oldName The name that will be replaced
	 * @param newName The name that will replace the old name
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void changeUserNames(String oldName, String newName, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// The updates can also be introduced as models
		ExampleUserModel updateModel = new ExampleUserModel();
		// Only the name attribute will be updated, so we only set the name attribute
		updateModel.setName(newName);
		
		// We can use the same where condition here as well
		Database.update(updateModel.toDatabaseModel(), 
				Option.some(ExampleConditions.createUserNameCondition(oldName)), false, connection);
	}
	
	/**
	 * This method deletes all users from the database, which have the specified user name 
	 * and role id
	 * @param userName The required user name
	 * @param roleId The required role index
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void deleteUsers(String userName, int roleId, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// Using delete is pretty straightforward. One must simply specify the where condition 
		// that is used for selecting the affected rows
		Database.delete(ExampleTables.USERS, 
				Option.some(ExampleConditions.createUserNameAndRoleCondition(userName, roleId)), connection);
	}
	
	/**
	 * This method deletes a role and each user that uses that role
	 * @param roleName The name of the role that is deleted
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database was unavailable
	 */
	public static void deleteRole(String roleName, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		Condition roleNameCondition = ExampleConditions.createRoleNameCondition(roleName);
		
		// Deletes the users and roles which have the specified role name. A join condition is used.
		Database.delete(ExampleTables.USERS, 
				Join.createReferenceJoins(ExampleTables.USERS, ExampleTables.ROLES), 
				Option.some(roleNameCondition), true, connection);
	}
	
	private static ImmutableList<ExampleUserModel> parseUsers(ImmutableList<ImmutableList<ColumnVariable>> results)
	{
		// The result may contain data from multiple rows. Each row needs to be parsed 
		// separately. Also, each row contains a single user's attribute data
		return results.map(row -> new ExampleUserModel(row));
	}
}
