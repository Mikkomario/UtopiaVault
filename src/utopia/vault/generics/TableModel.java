package utopia.vault.generics;

import java.util.Collection;

import utopia.flow.generics.Model;
import utopia.flow.generics.Value;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * This model works like it's super class but also contains support methods for database 
 * interaction.
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class TableModel extends Model<ColumnVariable, Column>
{
	// ATTRIBUTES	---------------
	
	private Table table;
	
	
	// CONSTRUCTOR	---------------
	
	/**
	 * Creates a new empty model
	 * @param table The table the model uses
	 */
	public TableModel(Table table)
	{
		this.table = table;
	}

	/**
	 * Creates a new model with existing set of attributes
	 * @param table The table the model uses
	 * @param variables The variables stored in the model
	 */
	public TableModel(Table table, Collection<? extends ColumnVariable> variables)
	{
		super(variables);
		this.table = table;
	}

	/**
	 * Creates a copy of another database model
	 * @param other Another database model
	 */
	public TableModel(TableModel other)
	{
		super(other);
		this.table = other.table;
	}
	
	
	// IMPLEMENTED METHODS	--------------

	@Override
	protected ColumnVariable generateAttribute(ColumnVariable attribute)
	{
		return new ColumnVariable(attribute);
	}

	@Override
	protected ColumnVariable generateAttribute(String attributeName,
			Value value) throws NoSuchColumnException
	{
		return ColumnVariable.createVariable(getTable(), attributeName, value);
	}
	
	@Override
	public ColumnVariable getAttribute(String attributeName) throws NoSuchColumnException
	{
		// Tries to find the correct attribute
		ColumnVariable var = findAttribute(attributeName);
		
		// If the attribute couldn't be found, generates a new one with the columns default 
		// value
		if (var == null)
		{
			return getAttribute(attributeName, 
					getTable().findColumnWithVariableName(attributeName).getDefaultValue());
		}
		else
			return var;
	}
	
	
	// ACCESSORS	-------------------
	
	/**
	 * @return The database table the model uses
	 */
	public Table getTable()
	{
		return this.table;
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * Assigns a new value for a model's attribute. If the attribute didn't exist in the 
	 * model previously, it is added
	 * @param attributeName The name of the attribute
	 * @param value The value assigned to the attribute
	 * @throws NoSuchColumnException If the model's table doesn't contain a column for the 
	 * attribute
	 */
	public void setAttributeValue(String attributeName, Value value) throws NoSuchColumnException
	{
		setAttributeValue(attributeName, value, true);
	}
	
	/**
	 * Finds an attribute for a specific column
	 * @param column The column for which an attribute is requested
	 * @return An attribute used for the provided column
	 */
	public ColumnVariable getAttribute(Column column)
	{
		return getAttribute(column.getName());
	}
	
	/**
	 * Checks whether the model contains an attribute for the provided column
	 * @param column A column
	 * @return Does the model contain an attribute for the provided column
	 */
	public boolean containsAttribute(Column column)
	{
		return containsAttribute(column.getName());
	}
	
	/**
	 * @return The index attribute of this model
	 * @throws NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	public ColumnVariable getIndexAttribute() throws NoSuchColumnException
	{
		return getAttribute(getTable().getPrimaryColumn());
	}
	
	/**
	 * Changes the model's index
	 * @param index The new index value for the model
	 * @throws NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	public void setIndex(Value index) throws NoSuchColumnException
	{
		addAttribute(getTable().getPrimaryColumn().assignValue(index), true);
	}
	
	/**
	 * @return The model's index, which is the value of the attribute representing the primary 
	 * column of the model's table
	 * @throws NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	public Value getIndex() throws NoSuchColumnException
	{
		return getIndexAttribute().getValue();
	}
	
	/**
	 * Checks whether the model has a specified index attribute
	 * @return Does the model have a specified index attribute
	 */
	public boolean hasIndex()
	{
		if (getTable().findPrimaryColumn() == null)
			return false;
		else
			return containsAttribute(getTable().findPrimaryColumn());
	}
}
