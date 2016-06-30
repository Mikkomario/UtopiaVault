package utopia.vault.generics;

import java.util.Collection;

import utopia.flow.generics.Model;
import utopia.flow.generics.Value;
import utopia.flow.generics.VariableDeclaration;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * This model works like it's super class but also contains support methods for database 
 * interaction.
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class TableModel extends Model<ColumnVariable>
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
		super(new ColumnVariableParser(table));
		this.table = table;
	}

	/**
	 * Creates a new model with existing set of attributes
	 * @param table The table the model uses
	 * @param variables The variables stored in the model
	 */
	public TableModel(Table table, Collection<? extends ColumnVariable> variables)
	{
		super(new ColumnVariableParser(table), variables);
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
	public ColumnVariable getAttribute(VariableDeclaration column)
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
	 * Finds the model's index value. If there is no index assigned, the provided value is 
	 * returned instead
	 * @param defaultValue The value that should be returned when the model doesn't have an 
	 * index
	 * @return The model's index value or the provided value if the model doesn't have an index
	 */
	public Value getIndex(Value defaultValue)
	{
		if (hasIndex())
			return getIndex();
		else
			return defaultValue;
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
			return containsAttribute(getTable().findPrimaryColumn()).toBoolean();
	}
}
