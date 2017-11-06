package utopia.vault.generics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import utopia.flow.generics.BasicVariableParser;
import utopia.flow.generics.DeclarationVariableParser;
import utopia.flow.generics.Model;
import utopia.flow.generics.Model.NoSuchAttributeException;
import utopia.vault.generics.Table.NoSuchColumnException;
import utopia.flow.generics.ModelDeclaration;
import utopia.flow.generics.Value;
import utopia.flow.generics.Variable;
import utopia.flow.generics.VariableDeclaration;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.ImmutableMap;
import utopia.flow.util.Option;

/**
 * A combined model may contain both "normal" and database specific variables
 * @author Mikko Hilpinen
 * @since 10.1.2016
 */
public class CombinedModel
{
	// ATTRIBUTES	----------------
	
	private Model<Variable> baseModel;
	private TableModel dbModel;
	
	
	// CONSTRUCTOR	----------------
	
	/**
	 * Creates a new empty model
	 * @param table The table the model uses
	 */
	public CombinedModel(Table table)
	{
		this.baseModel = Model.createBasicModel();
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
			this.baseModel = Model.createBasicModel();
		else
			this.baseModel = new Model<>(new BasicVariableParser(), otherVariables);
		
		if (databaseVariables == null)
			this.dbModel = new TableModel(table);
		else
			this.dbModel = new TableModel(table, databaseVariables);
	}
	
	/**
	 * Creates a new model with existing attributes
	 * @param table The table the model uses
	 * @param databaseVariables The database-specific model attributes (optional)
	 * @param otherVariables The general model attributes (optional)
	 */
	public CombinedModel(Table table, 
			ImmutableList<? extends ColumnVariable> databaseVariables, 
			ImmutableList<? extends Variable> otherVariables)
	{
		if (otherVariables == null)
			this.baseModel = Model.createBasicModel();
		else
			this.baseModel = new Model<>(new BasicVariableParser(), otherVariables);
		
		if (databaseVariables == null)
			this.dbModel = new TableModel(table);
		else
			this.dbModel = new TableModel(table, databaseVariables);
	}
	
	// IMPLEMENTED METHODS	----------
	
	@Override
	public String toString()
	{
		return ImmutableMap.withValue("Database attributes", this.dbModel.getValuesMap(false)).plus(
				"Other attributes", this.baseModel.getValuesMap(false)).toString();
		
		/*
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
		for (Variable var : getGeneralAttributes())
		{
			s.append("\n");
			s.append(var);
		}
		
		return s.toString();
		*/
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
	 * The returned model is able to generate new variables based on this model's table.
	 */
	public Model<Variable> toModel()
	{
		return new Model<>(new DeclarationVariableParser<>(getTable().toModelDeclaration(), 
				false, new BasicVariableParser()), getAttributes());
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
	public ImmutableList<Variable> getGeneralAttributes()
	{
		return this.baseModel.getAttributes();
	}
	
	/**
	 * @return The model's database attributes
	 */
	public ImmutableList<ColumnVariable> getDatabaseAttributes()
	{
		return this.dbModel.getAttributes();
	}
	
	/**
	 * @return Both the models database attributes and the other attributes
	 */
	public ImmutableList<Variable> getAttributes()
	{
		return getGeneralAttributes().plus(getDatabaseAttributes());
	}
	
	public Option<Value> find(String attributeName)
	{
		Option<Value> dbValue = this.dbModel.find(attributeName);
		if (dbValue.isDefined())
			return dbValue;
		else
			return this.baseModel.find(attributeName);
	}
	
	public Option<? extends Variable> findAttribute(String attributeName)
	{
		Option<ColumnVariable> dbAtt = this.dbModel.findAttribute(attributeName);
		if (dbAtt.isDefined())
			return dbAtt;
		else
			return this.baseModel.findAttribute(attributeName);
	}
	
	public Value get(String attributeName)
	{
		return find(attributeName).getOrElse(Value.EMPTY);
	}
	
	/**
	 * @param attributeName The name of the attribute
	 * @return A database-related value for the attribute or an empty value if no such attribute exists
	 */
	public Value getDatabaseValue(String attributeName)
	{
		return this.dbModel.get(attributeName);
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
	 * Finds one of the model's database attributes
	 * @param column The column associated with the attribute
	 * @return The model's attribute associated with the column
	 */
	public ColumnVariable getDatabaseAttribute(Column column)
	{
		return this.dbModel.getAttribute(column);
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
	 * Finds the value of a single attribute in the model. Short for calling 
	 * getAttribute(String).getValue().
	 * @param attributeName The name of the attribute
	 * @return The value of the attribute
	 * @throws NoSuchColumnException If the model doesn't contain such an attribute and 
	 * neither does the model's table
	 */
	public Value getAttributeValue(String attributeName) throws NoSuchColumnException
	{
		return getAttribute(attributeName).getValue();
	}
	
	/**
	 * @return Whether the model has an attribute corresponding to the table's index / primary 
	 * column
	 */
	public boolean hasIndex()
	{
		return toDatabaseModel().hasIndex();
	}
	
	/**
	 * @return The model's index attribute (the one associated with the table's primary key)
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 */
	public ColumnVariable getIndexAttribute() throws NoSuchColumnException
	{
		return this.dbModel.getIndexAttribute();
	}
	
	/**
	 * Retrieves the model's index attribute's value. Short for using getIndexAttribute().getValue()
	 * @return The model's index
	 */
	public Value getIndex()
	{
		return this.dbModel.getIndex();
	}
	
	/**
	 * Retrieves the model's index attribute's value. If the model doesn't have an index 
	 * value, the provided default value will be returned instead.
	 * @param defaultValue The default value that will be returned when the model doesn't 
	 * have an index
	 * @return The model's index attirbute's value or, if that's not available, the default value
	 */
	public Value getIndex(Value defaultValue)
	{
		return this.dbModel.getIndex(defaultValue);
	}
	
	/**
	 * Changes the value of the model's index attribute
	 * @param index The new index for the model
	 */
	public void setIndex(Value index)
	{
		this.dbModel.setIndex(index);
	}
	
	/**
	 * Changes an attribute value of the model. The targeted value may be a database attribute 
	 * or a general attribute
	 * @param attributeName The name of the attribute
	 * @param value The new value assigned to the attribute
	 * @throws NoSuchAttributeException If the table doesn't contain the attribute and it 
	 * couldn't be generated either
	 */
	public void setAttributeValue(String attributeName, Value value) throws NoSuchAttributeException
	{
		Variable var = this.baseModel.findAttribute(attributeName);
		if (var == null)
		{
			if (getTable().containsColumnForVariable(attributeName))
				this.dbModel.setAttributeValue(attributeName, value);
			else
				this.baseModel.setAttributeValue(attributeName, value);
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
	 * This metod adds multiple new general attributes to the model. If the model already 
	 * contains some of the attributes, the previous attributes get overwritten
	 * @param attributes The attributes that are added to the model
	 */
	public void addGeneralAttributes(Collection<? extends Variable> attributes)
	{
		this.baseModel.addAttributes(attributes, true);
	}
	
	/**
	 * @return A model declaration of this model. The declaration contains declarations for 
	 * all attributes used by this model.
	 */
	public ModelDeclaration getModelDeclaration()
	{
		List<VariableDeclaration> declarations = new ArrayList<>();
		for (Variable var : getAttributes())
		{
			declarations.add(var.getDeclaration());
		}
		
		return new ModelDeclaration(declarations);
	}
}
