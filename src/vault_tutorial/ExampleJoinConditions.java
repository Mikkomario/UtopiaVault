package vault_tutorial;

import vault_database_old.DatabaseAccessor;
import vault_database_old.DatabaseAccessor.JoinCondition;

/**
 * This is a static collection that contains example methods for creating join conditions.
 * @author Mikko Hilpinen
 * @since 24.11.2015
 * @deprecated Please user {@link ExampleConditions#createRoleIndexJoinCondition()} instead
 */
public class ExampleJoinConditions
{
	// CONSTRUCTOR	----------------
	
	private ExampleJoinConditions()
	{
		// The interface is static so no constructor is needed
	}

	
	// OTHER METHODS	------------
	
	/**
	 * Creates a join condition that joins the correct user role to a user row. Attribute 
	 * names are used when creating the condition.
	 * @return A new join condition
	 */
	public static JoinCondition createUserRoleAttributeNameJoinCondition()
	{
		// Attribute name join condition is easy to create, but hard-coded values are a negative
		// Also, when using this condition, the first table must be USERS and the joined 
		// table must be ROLES
		return new DatabaseAccessor.AttributeNameJoinCondition("role", "id");
	}
	
	/**
	 * Creates a join condition that joins the correct user role to a user row. Column 
	 * names are used when creating the condition.
	 * @return A new join condition
	 */
	public static JoinCondition createUserRoleColumnNameJoinCondition()
	{
		// The SimpleJoinCondition faces the same problems as the Attribute name join condition,
		// Only this time one must use the column names instead of the attribute names
		// Usually it is better to use the AttributeNameJoinCondition instead
		return new DatabaseAccessor.SimpleJoinCondition("users_role", "roles_id");
	}
}
