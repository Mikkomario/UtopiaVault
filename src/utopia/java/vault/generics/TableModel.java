package utopia.java.vault.generics;

import java.util.Collection;

import utopia.java.flow.generics.Model;
import utopia.java.flow.generics.Value;
import utopia.java.flow.generics.VariableDeclaration;
import utopia.java.flow.generics.VariableParser;
import utopia.java.flow.structure.ImmutableList;
import utopia.java.flow.structure.Option;

/**
 * This model works like it's super class but also contains support methods for database 
 * interaction.
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class TableModel extends Model<ColumnVariable> implements IndexedModel
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
	 * @deprecated ImmutableList implementation replaces this method
	 */
	public TableModel(Table table, Collection<? extends ColumnVariable> variables)
	{
		super(new ColumnVariableParser(table), variables);
		this.table = table;
	}
	
	/**
	 * Creates a new model with existing set of attributes
	 * @param table The table the model uses
	 * @param variables The variables stored in the model (only table specific variables will be added)
	 */
	public TableModel(Table table, ImmutableList<ColumnVariable> variables)
	{
		super(new ColumnVariableParser(table), variables.filter(v -> table.equals(v.getColumn().getTable())));
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
	
	/**
	 * Creates a new table model with the specified variable parser. The parser should be 
	 * able to generate variables for each of the provided table's columns
	 * @param table The table used by this model
	 * @param parser The variable parser used by this model
	 */
	protected TableModel(Table table, VariableParser<? extends ColumnVariable> parser)
	{
		super(parser);
		this.table = table;
	}
	
	
	// IMPLEMENTED METHODS	-----------
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((this.table == null) ? 0 : this.table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof TableModel))
			return false;
		TableModel other = (TableModel) obj;
		if (this.table == null)
		{
			if (other.table != null)
				return false;
		}
		else if (!this.table.equals(other.table))
			return false;
		return true;
	}
	
	/**
	 * Changes the model's index
	 * @param index The new index value for the model
	 * @throws Table.NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	@Override
	public void setIndex(Value index) throws Table.NoSuchColumnException
	{
		addAttribute(getTable().getPrimaryColumn().assignValue(index), true);
	}
	
	/**
	 * @return The index value for the model or none if no such attribute exists
	 */
	@Override
	public Option<Value> getIndexOption()
	{
		return getIndexAttributeOption().map(att -> att.getValue());
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
	 * @throws Table.NoSuchColumnException If the model's table doesn't have an index attribute
	 */
	public ColumnVariable getIndexAttribute() throws Table.NoSuchColumnException
	{
		return getAttribute(getTable().getPrimaryColumn());
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
	 * @return The index attribute for the model or None if no such attribute exists
	 */
	public Option<ColumnVariable> getIndexAttributeOption()
	{
		return getTable().findPrimaryColumn().flatMap(column -> findAttribute(column.getName()));
	}
	
	/**
	 * Checks whether the model has a specified index attribute and that attribute is not null
	 * @return Does the model have a specified index attribute
	 */
	public boolean hasIndex()
	{
		return getIndexAttributeOption().exists(att -> !att.isNull());
	}
}
