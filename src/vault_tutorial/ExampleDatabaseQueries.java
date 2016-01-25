package vault_tutorial;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import utopia.flow.generics.Value;
import vault_database.Condition;
import vault_database.Database;
import vault_database.DatabaseException;
import vault_database_old.DatabaseUnavailableException;
import vault_generics.Column;
import vault_generics.ColumnVariable;
import vault_generics.Table;

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
	 * @return The user with the provided index or null if no such user exists
	 * @throws DatabaseException if the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static ExampleUserModel findUser(int userId) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// The model is first created as empty
		ExampleUserModel user = new ExampleUserModel();
		// The method finds the first row with the given index and reads the data into the 
		// user model. If the operation was a success, the model is returned
		if (Database.readModelAttributes(user.toDatabaseModel(), Value.Integer(userId)))
			return user;
		// It is often better to return null than an empty model
		return null;
	}
	
	/**
	 * This method finds and parses all user data stored in the database
	 * @return All users stored in the database
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static List<ExampleUserModel> findUsers() throws DatabaseException, DatabaseUnavailableException
	{
		// One can construct models with partial selects, but select all (null parameter) 
		// is often the best option
		List<List<ColumnVariable>> results = Database.select(null, ExampleTables.USERS, null, 
				null, null, -1, null);
		
		// The users are parsed in a separate method to avoid repeating code
		return parseUsers(results);
	}
	
	/**
	 * This method finds and parses all users stored in the database that have the provided 
	 * name.
	 * @param userName The required user name
	 * @return All users in the database that have the provided name
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed
	 */
	public static List<ExampleUserModel> findUsers(String userName) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// Like previously, all data is selected. This time a where condition is used, though.
		List<List<ColumnVariable>> results = Database.select(null, ExampleTables.USERS, null, 
				null, ExampleConditions.createUserNameCondition(userName), -1, null);
		
		// The users are parsed just like previously
		return parseUsers(results);
	}
	
	/**
	 * This method finds and parses all users that have the provided name and a role with 
	 * the provided name
	 * @param userName The required user name
	 * @param roleName The required role name
	 * @return All users which have the required attributes
	 * @throws DatabaseException if the operation fails
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static List<ExampleUserModel> findUsers(String userName, String roleName) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// This time only the user attributes are selected, as opposed to all, which 
		// would select the role attributes as well (since the roles are joined)
		List<? extends Column> selection = ExampleTables.USERS.getColumns();
		
		// A join condition is used. The condition needs to be wrapped into an array since 
		// the method expects one or more conditions
		// We can use a where condition that was specified in the ExampleWhereConditions
		List<List<ColumnVariable>> results = Database.select(
				selection, ExampleTables.USERS, new Table[] {ExampleTables.ROLES}, 
				new Condition[] {ExampleConditions.createRoleIndexJoinCondition()}, 
				ExampleConditions.createUserNameAndRoleNameCondition(userName, roleName), -1, 
				null);
		
		// The users are parsed the same way as always
		return parseUsers(results);
	}
	
	/**
	 * This method finds all the user names that exist in the database
	 * @return A set containing each user name in the database
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static Set<String> findUserNames() throws DatabaseException, 
			DatabaseUnavailableException
	{
		// This time only the user name is selected
		List<Column> selection = ExampleTables.USERS.getVariableColumns("name");
		// No where clause is used this time. The results come in same format as always
		List<List<ColumnVariable>> results = Database.select(selection, ExampleTables.USERS, 
				null, null, null, -1, null);
		
		Set<String> userNames = new HashSet<>();
		// Each list in the result represents a row in the database
		// Each row then contains a single user name
		for (List<ColumnVariable> row : results)
		{
			// The user name is the first and only attribute that is returned for each row.
			// That's why we can simply call get(0).
			// The attribute value must also be parsed to string before it is added to the set
			userNames.add(row.get(0).getValue().toString());
		}
		
		return userNames;
	}
	
	/**
	 * This method creates and inserts a user to the database
	 * @param userName The name of the user
	 * @param userRoleId The identifier of the user's role
	 * @return The user that was inserted
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 */
	public static ExampleUserModelOld insertUser(String userName, int userRoleId) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// The user is simply created as a new user model
		ExampleUserModelOld user = new ExampleUserModelOld();
		user.setName(userName);
		user.setRoleId(userRoleId);
		
		// The model can be easily inserted to the database. And since it doesn't have a 
		// specified index, it can't already exist in the database
		// The model also receives its index attribute (id) when this method is called
		DatabaseAccessor.insertObjectToDatabaseIfNotExists(user);
		
		return user;
	}
	
	/**
	 * Replaces a user name with another in the database
	 * @param oldName The name that will be replaced
	 * @param newName The name that will replace the old name
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void changeUserNames(String oldName, String newName) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// The updates can also be introduced as models
		ExampleUserModelOld updateModel = new ExampleUserModelOld();
		// Only the name attribute will be updated, so we only set the name attribute
		updateModel.setName(newName);
		
		// We can use the same where condition here as well
		DatabaseAccessor.update(updateModel, 
				ExampleConditions.createUserNameCondition(oldName), false);
	}
	
	/**
	 * This method deletes all users from the database, which have the specified user name 
	 * and role id
	 * @param userName The required user name
	 * @param roleId The required role index
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void deleteUsers(String userName, int roleId) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// Using delete is pretty straightforward. One must simply specify the where condition 
		// that is used for selecting the affected rows
		DatabaseAccessor.delete(ExampleTable.USERS, 
				ExampleConditions.createUserNameAndRoleCondition(userName, roleId));
	}
	
	/**
	 * This method deletes a role and each user that uses that role
	 * @param roleName The name of the role that is deleted
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database was unavailable
	 * @throws NoAttributeForColumnException If attribute name mapping fails
	 */
	public static void deleteRole(String roleName) throws DatabaseException, DatabaseUnavailableException, NoAttributeForColumnException
	{
		// Since join on delete is not yet supported, we have to select the deleted lines 
		// with a separate query
		// We only need to select the line indices for the deletion
		List<AttributeDescription> selection = Attribute.getPrimaryColumnDescription(
				ExampleTable.USERS).wrapIntoList();
		List<List<Attribute>> results = DatabaseAccessor.select(selection, 
				ExampleTable.USERS, ExampleTable.ROLES, 
				ExampleJoinConditions.createUserRoleAttributeNameJoinCondition().wrapIntoList(), 
				ExampleConditions.createRoleNameCondition(roleName), -1, 
				null);
		
		List<Attribute> indicesToBeDeleted = new ArrayList<>();
		for (List<Attribute> row : results)
		{
			indicesToBeDeleted.addAll(row);
		}
		
		// Now that we have the indices that can be deleted, we can perform the delete query
		DatabaseAccessor.delete(ExampleTable.USERS, 
				EqualsWhereCondition.createWhereModelAttributesCondition(Operator.EQUALS, 
				indicesToBeDeleted, true, CombinationOperator.OR));
	}
	
	private static List<ExampleUserModel> parseUsers(List<List<ColumnVariable>> results)
	{
		List<ExampleUserModel> users = new ArrayList<>();
		// The result may contain data from multiple rows. Each row needs to be parsed 
		// separately. Also, each row contains a single user's attribute data
		for (List<ColumnVariable> userAttributes : results)
		{
			// Since we created the attribute constructor, creating a new user is a simple 
			// matter
			users.add(new ExampleUserModel(userAttributes));
		}
		
		return users;
	}
}
