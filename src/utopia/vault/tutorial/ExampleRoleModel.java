package utopia.vault.tutorial;

import java.util.Collection;

import utopia.flow.generics.Value;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.TableModel;

/**
 * This model class only contains database-specific attributes, which is why it extends the 
 * table model class.
 * @author Mikko Hilpinen
 * @since 23.1.2016
 */
public class ExampleRoleModel extends TableModel
{
	// CONSTRUCTOR	--------------
	
	/**
	 * Creates a new empty role model
	 */
	public ExampleRoleModel()
	{
		super(ExampleTables.ROLES);
	}

	/**
	 * Creates a new role model with existing attributes
	 * @param variables The attributes assigned to the model
	 */
	public ExampleRoleModel(Collection<? extends ColumnVariable> variables)
	{
		super(ExampleTables.ROLES, variables);
	}
	
	
	// ACCESSORS	--------------
	
	/**
	 * @return The name of the role
	 */
	public String getName()
	{
		return getAttributeValue("name").toString();
	}
	
	/**
	 * Changes the name of the role
	 * @param name The new name of the role
	 */
	public void setName(String name)
	{
		setAttributeValue("name", Value.String(name));
	}
}
