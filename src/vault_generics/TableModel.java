package vault_generics;

import java.util.Collection;

import vault_generics.Table.NoSuchColumnException;
import flow_generics.Model;
import flow_generics.Value;

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
			Value value)
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
	 * Finds an attribute for a specific column
	 * @param column The column for which an attribute is requested
	 * @return An attribute used for the provided column
	 */
	public ColumnVariable getAttribute(Column column)
	{
		return getAttribute(column.getName());
	}
	
	/**
	 * @return The index attribute of this model
	 * @throws NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	public ColumnVariable getIndexAttribute() throws NoSuchColumnException
	{
		return getAttribute(getTable().getPrimaryColumn());
	}
}
