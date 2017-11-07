package utopia.vault.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.Value;
import utopia.flow.generics.VariableParser;
import utopia.flow.structure.ImmutableList;
import utopia.flow.util.Option;
import utopia.vault.database.CombinedCondition.CombinationOperator;
import utopia.vault.database.Join.JoinType;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.NoSuchReferenceException;
import utopia.vault.generics.Table;
import utopia.vault.generics.Table.NoSuchColumnException;
import utopia.vault.generics.TableModel;
import utopia.vault.generics.TableReference;

/**
 * This table model reads its data dynamically from the database. It also offers other utility 
 * methods for database interaction
 * @author Mikko Hilpinen
 * @since 23.7.2016
 */
public class DatabaseTableModel extends TableModel
{
	// ATTRIBUTES	--------------------
	
	private Database connection = null;
	private Column[] columnComboKeys;
	
	
	// CONSTRUCTOR	--------------------

	/**
	 * Creates a new model
	 * @param table The table used by the model
	 * @param comboIndexColumns The columns that when combined, create a unique index
	 */
	public DatabaseTableModel(Table table, Column... comboIndexColumns)
	{
		super(table);
		setVariableParser(new DatabaseVariableParser());
		this.columnComboKeys = comboIndexColumns;
	}
	
	/**
	 * Creates a new model with existing attributes
	 * @param table The table the model uses
	 * @param attributes The attributes assigned to the model
	 * @param comboIndexColumns The columns that when combined, create a unique index
	 */
	public DatabaseTableModel(Table table, Collection<? extends ColumnVariable> attributes, 
			Column... comboIndexColumns)
	{
		super(table);
		setVariableParser(new DatabaseVariableParser());
		this.columnComboKeys = comboIndexColumns;
		addAttributes(attributes, true);
	}
	
	/**
	 * Copies a table model into a database table model
	 * @param other Another model
	 * @param comboIndexColumns The columns that when combined, create a unique index
	 */
	public DatabaseTableModel(TableModel other, Column... comboIndexColumns)
	{
		super(other);
		setVariableParser(new DatabaseVariableParser());
		this.columnComboKeys = comboIndexColumns;
	}
	
	
	// ACCESSORS	--------------------
	
	/**
	 * Specifies that the model should use a specific database connection when reading or 
	 * writing table data. Always remember to close the connection calling {@link #closeConnection()} 
	 * in a final segment
	 * @param connection The database connection the model uses
	 */
	public void setConnection(Database connection)
	{
		this.connection = connection;
	}
	
	
	// OTHER METHODS	----------------
	
	/**
	 * This method closes any connection set to this model. The connection won't be used 
	 * by this model again.
	 * @see #setConnection(Database)
	 */
	public void closeConnection()
	{
		if (this.connection != null)
		{
			this.connection.closeConnection();
			this.connection = null;
		}
	}
	
	/**
	 * Reads the model's current status from the database. If no connection is opened for the 
	 * model, a temporary connection will be used.
	 * @return Was any data read. False will be returned if the model didn't have proper index 
	 * of if the model's data couldn't be found from the database.
	 * @throws DatabaseException If the query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public boolean readFromDatabase() throws DatabaseException, DatabaseUnavailableException
	{
		// Searches the model's data based on the index
		Condition condition = getCondition();
		if (condition == null)
			return false;
		else
			return Database.readModelAttributes(this, condition, this.connection);
	}
	
	/**
	 * Inserts this model to the database as a new row
	 * @throws DatabaseException If the insert failed. If model didn't contain all required attributes
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public void insertToDatabase() throws DatabaseException, DatabaseUnavailableException
	{
		Database.insert(this, this.connection);
	}
	
	/**
	 * Updates the model's current status to the database
	 * @param skipNullUpdates Should null assignments be skipped
	 * @return Was any update query performed
	 * @throws DatabaseException If the query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public boolean updateToDatabase(boolean skipNullUpdates) throws DatabaseException, 
			DatabaseUnavailableException
	{
		Condition condition = getCondition();
		if (condition == null)
			return false;
		else
		{
			Database.update(this, condition, skipNullUpdates, this.connection);
			return true;
		}
	}
	
	/**
	 * Updates the model's current status to the database. If the model didn't exist in the 
	 * database yet, performs an insert instead
	 * @param skipNullUpdates Should null value updates be skipped
	 * @throws DatabaseException If a query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public void insertOrUpdate(boolean skipNullUpdates) throws DatabaseException, 
			DatabaseUnavailableException
	{
		if (hasIndex())
		{
			// In auto-increment tables, the index is acquired with an insert
			if (getTable().usesAutoIncrementIndexing())
				Database.update(this, skipNullUpdates, this.connection);
			// In other tables the index row may or may not exist
			else
				Database.insertOrUpdate(this, skipNullUpdates, this.connection);
		}
		else
		{
			// Has to check whether combo key row already exists in the database
			Condition condition = getCondition();
			if (condition == null)
				Database.insert(this, this.connection);
			else if (Database.rowExists(getTable(), condition, this.connection))
				Database.update(this, condition, skipNullUpdates, this.connection);
			else
				Database.insert(this, this.connection);
		}
	}
	
	/**
	 * Checks whether this model exists in the database
	 * @return Is there a row for this model in the database
	 * @throws DatabaseException If the query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public boolean existsInDatabase() throws DatabaseException, DatabaseUnavailableException
	{
		Condition condition = getCondition();
		if (condition == null)
			return false;
		else
			return Database.rowExists(getTable(), condition, this.connection);
	}
	
	/**
	 * Finds the model referred by this model
	 * @param modelTable The model's table
	 * @return The model referred from this model or null if no such model could be found
	 * @throws DatabaseException If the query failed
	 * @throws NoSuchReferenceException If there isn't a reference from this model's table to 
	 * the provided table
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public TableModel getReferredModel(Table modelTable) throws DatabaseException, 
			NoSuchReferenceException, DatabaseUnavailableException
	{
		// Finds the reference between the tables
		ImmutableList<TableReference> references = getTable().getReferencesToTable(modelTable);
		if (references.isEmpty())
			throw new NoSuchReferenceException(getTable(), modelTable);
		else
		{
			// Finds the row referred from this model's row
			// If this model doesn't have a row, no joined row is found either
			Condition condition = getCondition();
			if (condition == null)
				return null;
			else
			{
				// Only accepts the first result from the first reference
				List<List<ColumnVariable>> results = Database.select(modelTable.getColumns(), 
						getTable(), ImmutableList.withValue(new Join(references.head(), JoinType.INNER)), 
						condition, 1, null, this.connection);
				if (results.isEmpty())
					return null;
				else
					return new TableModel(modelTable, results.get(0));
			}
		}
	}
	
	/**
	 * Finds all connected models from the provided table. In case there is a reference from 
	 * this table to the second, the result may contain the referred model. In case the 
	 * provided table refers this one, the result contains all models referring to this one.
	 * @param modelTable The table that is joined
	 * @return The referred / referring models from the provided table. The returned list 
	 * may be empty but not null.
	 * @throws DatabaseException If the query failed
	 * @throws NoSuchReferenceException If there isn't a reference between the two tables
	 * @throws DatabaseUnavailableException If the database coundn't be accessed
	 */
	public List<TableModel> getConnectedModels(Table modelTable) throws DatabaseException, 
			NoSuchReferenceException, DatabaseUnavailableException
	{
		Condition condition = getCondition();
		if (condition == null)
			return new ArrayList<>();
		else
		{
			// Joins the two tables together and finds the connected rows
			List<List<ColumnVariable>> results = Database.select(modelTable.getColumns(), 
					getTable(), Join.createReferenceJoins(getTable(), modelTable), condition, 
					-1, null, this.connection);
			
			// Parses the results
			List<TableModel> models = new ArrayList<>();
			for (List<ColumnVariable> row : results)
			{
				models.add(new TableModel(modelTable, row));
			}
			
			return models;
		}
	}
	
	private Condition getCondition()
	{
		// The primary key is the primary search option
		if (hasIndex())
			return ComparisonCondition.createIndexEqualsCondition(this);
		// May also use column combinations
		else if (this.columnComboKeys.length > 0)
		{
			// Must a value for each column
			Condition[] conditions = new Condition[this.columnComboKeys.length];
			for (int i = 0; i < conditions.length; i++)
			{
				Column column = this.columnComboKeys[i];
				Option<ColumnVariable> attribute = findAttribute(column.getName());
				if (attribute.isEmpty())
					return null;
				else
					conditions[i] = new ComparisonCondition(attribute.get());
			}
			
			return CombinedCondition.combineConditions(CombinationOperator.AND, conditions);
		}
		else
			return null;
	}

	
	// NESTED CLASSES	----------------
	
	private class DatabaseVariableParser implements VariableParser<ColumnVariable>
	{
		// IMPLEMENTED METHODS	--------
		
		@Override
		public ColumnVariable generate(String variableName)
				throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
		{
			try
			{
				// In case the table contains the variable, tries to generate it
				if (getTable().containsColumnForVariable(variableName))
				{
					// First tries reading the value from the database
					if (readFromDatabase())
						return getAttribute(variableName);
					// If that didn't work (index missing, etc.), generates the new value
					else
						return new ColumnVariable(getTable().getColumnWithVariableName(variableName));
				}
				else
					throw new VariableGenerationFailedException("Table " + getTable() + 
							" doesn't contain a column for variable " + variableName);
			}
			catch (DatabaseUnavailableException | DatabaseException | NoSuchColumnException | 
					utopia.flow.generics.Model.NoSuchAttributeException e)
			{
				throw new VariableGenerationFailedException(
						"Couldn't read attribute data from the database", e);
			}
		}

		@Override
		public ColumnVariable generate(String variableName, Value value)
				throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
		{
			// Assigns the provided value to the generated variable
			try
			{
				return new ColumnVariable(getTable().getColumnWithVariableName(variableName), 
						value);
			}
			catch (NoSuchColumnException e)
			{
				throw new VariableGenerationFailedException(
						"Couldn't find a column for the generated variable", e);
			}
		}

		@Override
		public ColumnVariable copy(ColumnVariable variable)
				throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
		{
			return new ColumnVariable(variable);
		}
	}
}
