package vault_tutorial;

import vault_database.EqualsWhereCondition;
import vault_database.EqualsWhereCondition.Operator;
import vault_database.WhereCondition;
import vault_tutorial.ExampleUserModel.ExampleRoleModel;

/**
 * This is a static collection of example methods that generate where conditions.
 * @author Mikko Hilpinen
 * @since 24.11.2015
 */
public class ExampleWhereConditions
{
	// CONSTRUCTOR	------------------
	
	private ExampleWhereConditions()
	{
		// The interface is static, so constructor isn't used
	}

	
	// OTHER METHODS	--------------
	
	/**
	 * This method generates a where condition that only selects user rows that have the 
	 * provided user name. In sql syntax, this would be something 
	 * like 'WHERE users_name <=> userName'
	 * @param userName The required user name
	 * @return A where condition that limits the rows to only those that have a the provided 
	 * user name
	 */
	public static WhereCondition createWhereUserNameEqualsWhereCondition(String userName)
	{
		// A good practice, where possible, is to create a model that contains the condition 
		// attributes. In this case it would be the 'name' attribute
		ExampleUserModel whereModel = new ExampleUserModel();
		whereModel.setName(userName);
		
		// This model can then be used for generating a WhereCondition using 
		// EqualsWhereCondition's static method.
		return EqualsWhereCondition.createWhereModelAttributesCondition(Operator.EQUALS, 
				whereModel.getAttributes(), false);
	}
	
	/**
	 * This method generates a where condition that only selects user rows that have the 
	 * specified name and role. In sql, this would be something like 'WHERE users_name <=> 
	 * name AND users_role <=> roleId'
	 * @param name The name requirement
	 * @param roleId The role requirement
	 * @return The user rows that have the specified name and role
	 */
	public static WhereCondition createWhereUserNameAndRoleEqualCondition(String name, 
			int roleId)
	{
		// One can add multiple conditions to the single model
		ExampleUserModel whereModel = new ExampleUserModel();
		// In this case, role and name are specified
		whereModel.setRoleId(roleId);
		whereModel.setName(name);
		
		// The returned where condition is actually a CombinedWhereCondition where the two 
		// conditions are combined with AND
		return EqualsWhereCondition.createWhereModelAttributesCondition(Operator.EQUALS, 
				whereModel.getAttributes(), false);
	}
	
	/**
	 * This method creates a where condition that only selects rows that have the specified 
	 * role name
	 * @param roleName The name in the selected role rows
	 * @return a where condition that only selects rows with the provided role name
	 */
	public static WhereCondition createWhereRoleNameEqualsWhereCondition(String roleName)
	{
		ExampleRoleModel whereRoleModel = new ExampleRoleModel();
		whereRoleModel.setName(roleName);
		return EqualsWhereCondition.createWhereModelAttributesCondition(Operator.EQUALS, 
				whereRoleModel.getAttributes(), false);
	}
	
	/**
	 * This method generates a where condition that selects rows that have the provided 
	 * user name and the provided role name. This where condition only works in join queries 
	 * where {@link ExampleTable#USERS} and {@link ExampleTable#ROLES} are joined.
	 * @param userName The user name requirement
	 * @param roleName The role name requirement
	 * @return A where condition that selects rows that have the provided user name and 
	 * the provided role name.
	 */
	public static WhereCondition createWhereUserNameAndRoleNameEqualWhereCondition(
			String userName, String roleName)
	{
		// Creates the two conditions like before, but this time combines them with AND
		return createWhereUserNameEqualsWhereCondition(userName).and(
				createWhereRoleNameEqualsWhereCondition(roleName));
	}
}
