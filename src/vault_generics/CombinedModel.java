package vault_generics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import vault_generics.Table.NoSuchColumnException;
import flow_generics.Model;
import flow_generics.Model.NoSuchAttributeException;
import flow_generics.ModelDeclaration;
import flow_generics.SimpleModel;
import flow_generics.Value;
import flow_generics.Variable;
import flow_generics.VariableDeclaration;

/**
 * A combined model may contain both "normal" and database specific variables
 * @author Mikko Hilpinen
 * @since 10.1.2016
 */
public class CombinedModel
{
	// ATTRIBUTES	----------------
	
	private SimpleModel baseModel;
	private TableModel dbModel;
	
	
	// CONSTRUCTOR	----------------
	
	/**
	 * Creates a new empty model
	 * @param table The table the model uses
	 */
	public CombinedModel(Table table)
	{
		this.baseModel = new SimpleModel();
		this.dbModel = new TableModel(table);
	}
	
	/**
	 * Creates a new model with existing attributes
	 * @param table The table the model uses
	 * @param databaseVariables The database-specific model attributes (optional)
	 * @param otherVariables The general model attributes (optional)
	 */
	public CombinedModel(Table table, 
			Collection<? extends ColumnVariable> databaseVariables, 
			Collection<? extends Variable> otherVariables)
	{
		if (otherVariables == null)
			this.baseModel = new SimpleModel();
		else
			this.baseModel = new SimpleModel(otherVariables);
		
		if (databaseVariables == null)
			this.dbModel = new TableModel(table);
		else
			this.dbModel = new TableModel(table, databaseVariables);
	}
	
	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();
		s.append("Model (");
		s.append(getTable());
		s.append(")\nDatabase attributes:");
		
		for (Variable var : getDatabaseAttributes())
		{
			s.append("\n");
			s.append(var);
		}
		
		s.append("\nOther attributes:");
		for (Variable var : getGenericAttributes())
		{
			s.append("\n");
			s.append(var);
		}
		
		return s.toString();
	}
	
	
	// ACCESSORS	------------------
	
	/**
	 * @return The database model version of this model. This model will only contain the 
	 * database attributes of this model. Modifying the model will affect this model as well.
	 */
	public TableModel toDatabaseModel()
	{
		return this.dbModel;
	}
	
	/**
	 * @return A generic model based on this model. The model will contain all attributes 
	 * introduced in this model, and modifying those attributes will also affect this model.
	 */
	public SimpleModel toSimpleModel()
	{
		return new SimpleModel(getAttributes());
	}
	
	
	// OTHER METHODS	--------------
	
	/**
	 * @return The database table the model uses
	 */
	public Table getTable()
	{
		return this.dbModel.getTable();
	}
	
	/**
	 * @return The model attributes which aren't database attributes
	 */
	public Set<Variable> getGenericAttributes()
	{
		return this.baseModel.getAttributes();
	}
	
	/**
	 * @return The model's database attributes
	 */
	public Set<ColumnVariable> getDatabaseAttributes()
	{
		return this.dbModel.getAttributes();
	}
	
	/**
	 * @return Both the models database attributes and the other attributes
	 */
	public Set<Variable> getAttributes()
	{
		Set<Variable> attributes = getGenericAttributes();
		attributes.addAll(getDatabaseAttributes());
		return attributes;
	}
	
	/**
	 * Finds one of the model's database attributes
	 * @param attributeName The name of the attribute
	 * @return an attribute from the model
	 * @throws NoSuchColumnException If the model didn't have such an attribute and one 
	 * couldn't be generated either
	 */
	public ColumnVariable getDatabaseAttribute(String attributeName) throws NoSuchColumnException
	{
		return this.dbModel.getAttribute(attributeName);
	}
	
	/**
	 * Finds one of the models attributes. The attribute may be a generic attribute or a 
	 * database attribute
	 * @param attributeName The name of the attribute
	 * @return The model's attribute with the provided name
	 * @throws NoSuchColumnException If the attribute couldn't be found nor generated
	 */
	public Variable getAttribute(String attributeName) throws NoSuchColumnException
	{
		Variable var = this.baseModel.findAttribute(attributeName);
		if (var == null)
			return getDatabaseAttribute(attributeName);
		else
			return var;
	}
	
	/**
	 * Changes an attribute value of the model. The targeted value may be a database attribute 
	 * or a general attribute
	 * @param attributeName The name of the attribute
	 * @param value The new value assigned to the attribute
	 * @throws NoSuchAttributeException If the table doesn't contain the attribute and it 
	 * hasn't been declared as a generic attribute either
	 */
	public void setAttribute(String attributeName, Value value) throws NoSuchAttributeException
	{
		Variable var = this.baseModel.findAttribute(attributeName);
		if (var == null)
		{
			if (getTable().containsColumnForVariable(attributeName))
				this.dbModel.setAttributeValue(attributeName, value, true);
			else
				throw new Model.NoSuchAttributeException(attributeName, this.baseModel);
		}
		else
			var.setValue(value);
	}
	
	/**
	 * This method adds a new general attribute to the model. If the model already contains 
	 * such an attribute, it is overwritten
	 * @param attribute The attribute that is added to the model
	 */
	public void addGeneralAttribute(Variable attribute)
	{
		this.baseModel.addAttribute(attribute, true);
	}
	
	/**
	 * @return A declaration for the database model part of this model. This declaration 
	 * will contain each column used in the model
	 */
	public TableModelDeclaration getColumnDeclaration()
	{
		List<Column> columns = new ArrayList<>();
		for (ColumnVariable var : getDatabaseAttributes())
		{
			columns.add(var.getColumn());
		}
		
		return new TableModelDeclaration(columns);
	}
	
	/**
	 * @return A model declaration of this model. The declaration contains declarations for 
	 * all attributes used by this model.
	 */
	public ModelDeclaration<VariableDeclaration> getModelDeclaration()
	{
		List<VariableDeclaration> declarations = new ArrayList<>();
		for (Variable var : getAttributes())
		{
			declarations.add(var.getDeclaration());
		}
		
		return new ModelDeclaration<>(declarations);
	}
}
