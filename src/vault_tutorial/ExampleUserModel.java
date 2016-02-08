package vault_tutorial;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.BasicDataType;
import utopia.flow.generics.Value;
import utopia.flow.generics.Variable;
import vault_database.Database;
import vault_database.DatabaseException;
import vault_database.DatabaseUnavailableException;
import vault_generics.ColumnVariable;
import vault_generics.CombinedModel;

/**
 * Models can be used as safe interfaces for database accessing. This model is used with the 
 * USERS table in the {@link ExampleTables}<br>
 * In addition to the column variables, the user model has a separate attribute "points"
 * @author Mikko Hilpinen
 * @since 21.1.2016
 */
public class ExampleUserModel extends CombinedModel
{
	// CONSTRUCTOR	---------------
	
	/**
	 * Creates a new empty user model
	 */
	public ExampleUserModel()
	{
		super(ExampleTables.USERS);
		
		addGeneralAttributes(initialiseGeneralAttributes());
	}

	/**
	 * Creates a new user model from the provided database variables
	 * @param databaseVariables The attributes that are added to the user
	 */
	public ExampleUserModel(Collection<? extends ColumnVariable> databaseVariables)
	{
		super(ExampleTables.USERS, databaseVariables, initialiseGeneralAttributes());
	}
	
	
	// ACCESSORS	---------------
	
	/**
	 * @return The name of the user
	 */
	public String getName()
	{
		return getAttributeValue("name").toString();
	}
	
	/**
	 * Changes the user's name
	 * @param name The new name of the user
	 */
	public void setName(String name)
	{
		setAttributeValue("name", Value.String(name));
	}
	
	/**
	 * @return The index of the user's role.
	 */
	public int getRoleIndex()
	{
		Value index = getAttributeValue("role");
		if (index.isNull())
			return 0;
		else
			return index.toInteger();
	}
	
	/**
	 * Changes the index of the users's role
	 * @param roleIndex The new index of the user's role
	 */
	public void setRoleIndex(int roleIndex)
	{
		setAttributeValue("role", Value.Integer(roleIndex));
	}
	
	/**
	 * @return The points the user has
	 */
	public int getPoints()
	{
		return getAttributeValue("points").toInteger();
	}
	
	/**
	 * Changes the amount of points the user has
	 * @param points The points the user has
	 */
	public void setPoints(int points)
	{
		setAttributeValue("points", Value.Integer(points));
	}
	
	/**
	 * Increases the amount of points the user has
	 * @param amount The amount of points added for the user
	 */
	public void increasePoints(int amount)
	{
		setPoints(getPoints() + amount);
	}

	
	// OTHER METHODS	-----------
	
	/**
	 * Reads the user's role from the database
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return The user's role
	 * @throws DatabaseException If the query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public ExampleRoleModel getRole(Database connection) throws DatabaseException, 
			DatabaseUnavailableException
	{
		Value roleId = getAttributeValue("roleId");
		if (!roleId.isNull())
		{
			ExampleRoleModel role = new ExampleRoleModel();
			if (Database.readModelAttributes(role, roleId, connection))
				return role;
		}
		
		return null;
	}
	
	private static List<Variable> initialiseGeneralAttributes()
	{
		List<Variable> vars = new ArrayList<>();
		vars.add(new Variable("points", BasicDataType.INTEGER, 0));
		
		return vars;
	}
}
