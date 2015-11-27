package vault_tutorial;

import java.util.Collection;

import vault_database.Attribute;
import vault_database.DatabaseAccessor;
import vault_database.DatabaseException;
import vault_database.DatabaseUnavailableException;
import vault_database.DatabaseValue;
import vault_recording.DatabaseModel;

/**
 * This is an example implementation for a subclass of the {@link DatabaseModel} class. 
 * The class provides easy-to-use accessors for the suparclasses attributes.
 * @author Mikko Hilpinen
 * @since 24.11.2015
 */
public class ExampleUserModel extends DatabaseModel
{
	// CONSTRUCTOR	--------------------
	
	/**
	 * This method creates an empty user model. It is useful (and easy) to provide a 
	 * constructor for for an empty model.
	 */
	public ExampleUserModel()
	{
		super(ExampleTable.USERS);
	}

	/**
	 * This method creates a user model with existing set of attributes. This is often used 
	 * when wrapping selected attributes into model form.
	 * @param attributes The attributes the user model will receive. Only table attributes 
	 * will be added.
	 */
	public ExampleUserModel(Collection<? extends Attribute> attributes)
	{
		super(ExampleTable.USERS, attributes);
	}

	
	// ACCESSORS	--------------------
	
	/**
	 * @return The user model's name value in string format
	 */
	public String getName()
	{
		// Setting the second parameter to 'true' means that
		// The name attribute will be generated to null if it hasn't yet been set
		// The value will be parsed to string
		return getAttribute("name", true).getValue().toString();
	}
	
	/**
	 * Updates the user's name attribute
	 * @param name The name of the user
	 */
	public void setName(String name)
	{
		// This method expects the provided value to be of correct data type (in this case 
		// String)
		// Setting the last parameter to 'true' means that
		// A new attribute will be generated or a previous attribute will be modified
		setAttributeValue("name", name, true);
	}
	
	/**
	 * @return The user model's row id, which is also the model's index
	 */
	public int getId()
	{
		return getAttribute("id", true).getValue().toInt();
	}
	
	/**
	 * Updates the model's id
	 * @param id The model's new id
	 */
	public void setId(int id)
	{
		setAttributeValue("id", id, true);
	}
	
	/**
	 * @return The index of the user's role
	 */
	public int getRoleId()
	{
		return getAttribute("role", true).getValue().toInt();
	}
	
	/**
	 * Updates the user model's role index
	 * @param roleId The model's new role index
	 */
	public void setRoleId(int roleId)
	{
		setAttributeValue("role", roleId, true);
	}
	
	/**
	 * @return Retrieves the user's role from the database. Null if the user role is 
	 * undefined or not found from the database
	 */
	public ExampleRoleModel getRole()
	{
		// Not all methods need to be simple accessors, and some may even include database 
		// queries
		try
		{
			// The role is first initialised as an empty model
			ExampleRoleModel role = new ExampleRoleModel();
			// Once the role's index (id) is known, it is easy to initialise the rest of the 
			// attributes using the database accessor
			// The indices data type is specified as an integer at this point
			if (DatabaseAccessor.readObjectAttributesFromDatabase(role, 
					DatabaseValue.Integer(getRoleId())))
				return role;
			// The method returns whether any data was found and read. If there wasn't any 
			// data in the database, it is often better to return null than an incomplete 
			// model
			else
				return null;
		}
		catch (DatabaseException | DatabaseUnavailableException e)
		{
			// One should handle error situations properly. In a getter like this one, the 
			// policy shouldn't be too strict, but it's up to the implementer
			return null;
		}
	}
	
	
	// SUBCLASSES	-------------------
	
	/**
	 * This is another example implementation of a model class. This time the model represents 
	 * the other table in the {@link ExampleTable}. The class is an inner class for 
	 * presentation purposes only.
	 * @author Mikko Hilpinen
	 * @since 24.11.2015
	 */
	public static class ExampleRoleModel extends DatabaseModel
	{
		// CONSTRUCTOR	-------------------
		
		/**
		 * Creates a new empty role model
		 */
		public ExampleRoleModel()
		{
			super(ExampleTable.ROLES);
		}
		
		/**
		 * Creates a new role model with existing attributes.
		 * @param attributes The attributes the model will receive. Only the attributes 
		 * from the {@link ExampleTable#ROLES} will be added
		 */
		public ExampleRoleModel(Collection<? extends Attribute> attributes)
		{
			super(ExampleTable.ROLES, attributes);
		}
		
		/**
		 * Creates a new role model with existing attributes
		 * @param index The role's index
		 * @param name The role's name
		 */
		public ExampleRoleModel(int index, String name)
		{
			super(ExampleTable.ROLES);
			
			// Sometimes it may be useful to allow the user to apply the correct attributes 
			// right in the constructor. Especially in more simple models.
			setId(index);
			setName(name);
		}
		
		
		// ACCESSORS	-----------------
		
		/**
		 * @return The row id / index of the role model
		 */
		public int getId()
		{
			return getAttribute("id", true).getValue().toInt();
		}
		
		/**
		 * Updates the model's id / index
		 * @param id The model's new index
		 */
		public void setId(int id)
		{
			setAttributeValue("id", id, true);
		}
		
		/**
		 * @return The role model's name value in string format
		 */
		public String getName()
		{
			return getAttribute("name", true).getValue().toString();
		}
		
		/**
		 * Updates the role's name attribute
		 * @param name The name of the role
		 */
		public void setName(String name)
		{
			setAttributeValue("name", name, true);
		}
	}
}
