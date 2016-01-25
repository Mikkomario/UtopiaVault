package vault_tutorial;

import vault_database.ComparisonCondition;
import vault_database.Condition;
import vault_generics.Column;

/**
 * This is a static collection of example methods that generate conditions that can be used 
 * in where adn join clauses.
 * @author Mikko Hilpinen
 * @since 24.11.2015
 */
public class ExampleConditions
{
	// CONSTRUCTOR	------------------
	
	private ExampleConditions()
	{
		// The interface is static, so constructor isn't used
	}

	
	// OTHER METHODS	--------------
	
	/**
	 * This method generates a condition that only selects user rows that have the 
	 * provided user name. In sql syntax, this would be something 
	 * like 'WHERE users_name <=> userName'
	 * @param userName The required user name
	 * @return A condition that limits the rows to only those that have the provided 
	 * user name
	 */
	public static Condition createUserNameCondition(String userName)
	{
		// A good practice, where possible, is to create a model that contains the condition 
		// attributes. In this case it would be the 'name' attribute
		ExampleUserModel whereModel = new ExampleUserModel();
		whereModel.setName(userName);
		
		// This model can then be used for generating a Condition
		// In the case of user model, the database model part must be separated from the 
		// combined model
		return ComparisonCondition.createModelEqualsCondition(whereModel.toDatabaseModel());
	}
	
	/**
	 * This method generates a condition that only selects user rows that have the 
	 * specified name and role. In sql, this would be something like 'WHERE users_name <=> 
	 * name AND users_role <=> roleId'
	 * @param name The name requirement
	 * @param roleId The role requirement
	 * @return A condition that returns user rows that have the specified name and role
	 */
	public static Condition createUserNameAndRoleCondition(String name, int roleId)
	{
		// One can add multiple conditions to the single model
		ExampleUserModel whereModel = new ExampleUserModel();
		// In this case, role and name are specified
		whereModel.setRoleIndex(roleId);
		whereModel.setName(name);
		
		// The returned where condition is actually a CombinedWhereCondition where the two 
		// conditions are combined with AND
		return ComparisonCondition.createModelEqualsCondition(whereModel.toDatabaseModel());
	}
	
	/**
	 * This method creates a condition that only selects rows that have the specified 
	 * role name
	 * @param roleName The name in the selected role rows
	 * @return a condition that only selects rows with the provided role name
	 */
	public static Condition createRoleNameCondition(String roleName)
	{
		ExampleRoleModel whereRoleModel = new ExampleRoleModel();
		whereRoleModel.setName(roleName);
		return ComparisonCondition.createModelEqualsCondition(whereRoleModel);
	}
	
	/**
	 * This method generates a condition that selects rows that have the provided 
	 * user name and the provided role name. This where condition only works in join queries 
	 * where users and roles are joined.
	 * @param userName The user name requirement
	 * @param roleName The role name requirement
	 * @return A condition that selects rows that have the provided user name and 
	 * the provided role name.
	 */
	public static Condition createUserNameAndRoleNameCondition(String userName, String roleName)
	{
		// Creates the two conditions like before, but this time combines them with AND
		return createUserNameCondition(userName).and(
				createRoleNameCondition(roleName));
	}
	
	/**
	 * This method creates a condition that can be used for joining users table with the 
	 * role table based on the user's role index attribute
	 * @return A condition the joins users and roles tables
	 */
	public static Condition createRoleIndexJoinCondition()
	{
		Column userRoleColumn = ExampleTables.USERS.findColumnWithVariableName("role");
		Column roleIndexColumn = ExampleTables.ROLES.getPrimaryColumn();
		
		return new ComparisonCondition(userRoleColumn, roleIndexColumn);
	}
}
