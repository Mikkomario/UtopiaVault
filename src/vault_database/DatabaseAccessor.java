package vault_database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import jdk.nashorn.internal.objects.annotations.Where;
import vault_database.Attribute.AttributeDescription;
import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.AttributeNameMapping.NoColumnForAttributeException;
import vault_database.DatabaseSettings.UninitializedSettingsException;
import vault_database.DatabaseTable.ColumnInfo;
import vault_recording.DatabaseReadable;
import vault_recording.DatabaseWritable;

/**
 * DatabaseAccessProvider is used to access and modify data in a database. 
 * The class provides an easy to use interface for executing statements in the 
 * database.
 * 
 * @author Mikko Hilpinen
 * @since 17.7.2014
 */
public class DatabaseAccessor
{
	// ATTRIBUTES	------------------------------------------------------
	
	private String databaseName;
	private Connection currentConnection;
	
	
	// CONSTRUCTOR	------------------------------------------------------
	
	/**
	 * Creates a new DatabaseAccessProvider that will allow the modification 
	 * of the specified database
	 * 
	 * @param databaseName The name of the database to be used initially
	 */
	public DatabaseAccessor(String databaseName)
	{
		// Initializes attributes
		this.databaseName = databaseName;
		
		this.currentConnection = null;
	}

	
	// OTHER METHODS	--------------------------------------------------
	
	/**
	 * Returns the sql connection used. Please note that the ownership of the connection stays 
	 * with this instance and will be closed when {@link #closeConnection()} is 
	 * called.
	 * @return java.sql.Connection The sql connection used by this instance. If the connection 
	 * is unusable, it is opened.
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws SQLException If the connection failed
	 */
	public java.sql.Connection getSQLConnection() throws DatabaseUnavailableException, SQLException
	{
		if (this.currentConnection == null)
			openConnection();
		else if (this.currentConnection.isClosed())
		{
			this.currentConnection = null;
			openConnection();
		}
		else if (!this.currentConnection.isValid(10))
		{
			closeConnection();
			openConnection();				
		}
		
		return this.currentConnection;
	}
	
	/**
	 * This method opens a connection to the formerly specified database. The 
	 * connection must be closed with {@link #closeConnection()} after the necessary 
	 * statements have been executed.
	 * @throws DatabaseUnavailableException If the connection couldn't be opened
	 * @throws SQLException If the operation failed
	 * @see #closeConnection()
	 */
	private void openConnection() throws DatabaseUnavailableException
	{
		// Tries to form a connection to the database
		try
		{
			// If a connection is already open, quits
			if (this.currentConnection != null && !this.currentConnection.isClosed())
				return;
			
			this.currentConnection = DriverManager.getConnection(
					DatabaseSettings.getConnectionTarget() + this.databaseName, 
					DatabaseSettings.getUser(), DatabaseSettings.getPassword());
		}
		catch (SQLException | UninitializedSettingsException e)
		{
			throw new DatabaseUnavailableException(e);
		}
	}
	
	/**
	 * Closes a currently open connection to the database.
	 */
	public void closeConnection()
	{
		// Closes the connection
		try
		{
			// If there is no connection, quits
			if (this.currentConnection == null || this.currentConnection.isClosed())
				return;
			
			this.currentConnection.close();
			this.currentConnection = null;
		}
		catch (SQLException e)
		{
			System.err.println("DatabaseAccessProvider failed to close a "
					+ "connection to " + this.databaseName);
			e.printStackTrace();
			return;
		}
	}
	
	/**
	 * Executes a simple INSERT, UPDATE or DELETE statement. If you want to 
	 * execute a query or check the results of your statement, use 
	 * {@link #getPreparedStatement(String, boolean)} instead.
	 * 
	 * @param sqlStatement The statement that will be executed in the current 
	 * database
	 * @throws SQLException If the statement was malformed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public void executeStatement(String sqlStatement) throws SQLException, 
			DatabaseUnavailableException
	{
		if (sqlStatement == null)
			return;
		
		Statement statement = null;
		try
		{
			statement = getSQLConnection().createStatement();
			statement.executeUpdate(sqlStatement);
		}
		finally
		{
			closeStatement(statement);
		}
	}
	
	/**
	 * This method creates and returns a preparedStatment to the database. The 
	 * returned statement must be closed with {@link #closeStatement(Statement)} 
	 * after it has been used.
	 * 
	 * @param sqlStatement The sql statement that will be prepared. The 
	 * statement may include multiple '?'s as placeholders for future parameters.
	 * @param returnAutogeneratedKeys Should the returned PreparedStatement be 
	 * able to return the auto generated keys created during the execution. (default: false)
	 * @return a PreparedStatement based on the given sql statement
	 * @throws DatabaseUnavailableException If the database couldn't be accessed 
	 * @throws SQLException If the statement was malformed
	 */
	public PreparedStatement getPreparedStatement(String sqlStatement, 
			boolean returnAutogeneratedKeys) throws SQLException, DatabaseUnavailableException
	{
		if (sqlStatement == null)
			throw new SQLException("No statement provided");
		
		PreparedStatement statement = null;
		
		int autoGeneratedKeys = Statement.NO_GENERATED_KEYS;
		if (returnAutogeneratedKeys)
			autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS;
		
		try
		{
			statement = getSQLConnection().prepareStatement(sqlStatement, 
					autoGeneratedKeys);
		}
		// Closes the statement if the operation fails
		catch (SQLException e)
		{
			closeStatement(statement);
			throw e;
		}
		catch (DatabaseUnavailableException e)
		{
			closeStatement(statement);
			throw e;
		}
		
		return statement;
	}
	
	/**
	 * This method creates and returns a preparedStatment to the database. The 
	 * returned statement must be closed with {@link #closeStatement(Statement)} 
	 * after it has been used.
	 * 
	 * @param sqlStatement The sql statement that will be prepared. The 
	 * statement may include multiple '?'s as placeholders for future parameters.
	 * @return a PreparedStatement based on the given sql statement
	 * @throws DatabaseUnavailableException If the database couldn't be accessed 
	 * @throws SQLException If the statement was malformed
	 */
	public PreparedStatement getPreparedStatement(String sqlStatement) 
			throws DatabaseUnavailableException, SQLException
	{
		return getPreparedStatement(sqlStatement, false);
	}
	
	/**
	 * Changes the database that is currently being used
	 * 
	 * @param newDatabaseName The name of the new database to be used
	 * @return The name of the database used after the call of this method
	 */
	public String changeDatabase(String newDatabaseName)
	{
		try
		{
			// The change is simple when a connection is closed
			if (this.currentConnection == null || this.currentConnection.isClosed())
				this.databaseName = newDatabaseName;
			// When a connection is open, informs the server
			else
			{
				executeStatement("USE " + newDatabaseName + ";");
				this.databaseName = newDatabaseName;	
			}
		}
		catch (SQLException | DatabaseUnavailableException e)
		{
			System.err.println("DatabaseAccessor failed to change the database name");
			e.printStackTrace();
		}
		
		return this.databaseName;
	}
	
	/**
	 * Closes a currently open statement
	 * 
	 * @param statement The statement that will be closed
	 */
	public static void closeStatement(Statement statement)
	{
		try
		{
			if (statement != null)
				statement.close();
		}
		catch (SQLException e)
		{
			System.err.println("DatabaseAccessProvider failed to close a statement");
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes a currently open resultSet
	 * 
	 * @param resultSet The results that will be closed
	 */
	public static void closeResults(ResultSet resultSet)
	{
		try
		{
			if (resultSet != null)
				resultSet.close();
		}
		catch (SQLException e)
		{
			System.err.println("DatabaseAccessProvider failed to close a resultSet");
			e.printStackTrace();
		}
	}
	
	/**
	 * Makes a select from query and returns the selection data
	 * @param selectedAttributes The attributes that will be selected
	 * @param fromTable The table the selection is made from
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static List<List<Attribute>> select(Collection<? extends AttributeDescription> 
			selectedAttributes, 
			DatabaseTable fromTable, int limit) throws 
			DatabaseUnavailableException, DatabaseException
	{
		return select(selectedAttributes, fromTable, null, limit, null);
	}
	
	/**
	 * Makes a select from query and returns the selection data
	 * @param selectedAttributes The attributes that will be selected
	 * @param fromTable The table the selection is made from
	 * @param where The where clause used in the select (null if all rows are selected)
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static List<List<Attribute>> select(
			Collection<? extends AttributeDescription> selectedAttributes, 
			DatabaseTable fromTable, WhereClause where, int limit) throws 
			DatabaseUnavailableException, DatabaseException
	{
		return select(selectedAttributes, fromTable, where, limit, null);
	}
	
	/**
	 * Makes a select from query and returns the selection data
	 * @param selectedAttributes The attributes that will be selected
	 * @param fromTable The table the selection is made from
	 * @param where The where clause used in the select (null if all rows are selected)
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @param extraSQL Extra sql code added to the end of the query. Can be, for example, 
	 * sorting information. Null if not used.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static List<List<Attribute>> select(
			Collection<? extends AttributeDescription> selectedAttributes, 
			DatabaseTable fromTable, WhereClause where, int limit, 
			String extraSQL) throws 
			DatabaseUnavailableException, DatabaseException
	{
		return select(selectedAttributes, fromTable, null, null, where, limit, 
				extraSQL);
	}
	
	/**
	 * Makes a select from query and returns the selection data
	 * @param selectedAttributes The attributes that will be selected
	 * @param fromTable The table the selection is made from
	 * @param joinTable The table that is joined into the selection (null if no join is made)
	 * @param joinConditions The conditions on which the rows are joined (null or empty if no 
	 * join is made)
	 * @param where The where clause used in the select (null if all rows are selected)
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @param extraSQL Extra sql code added to the end of the query. Can be, for example, 
	 * sorting information. Null if not used.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static List<List<Attribute>> select(Collection<? extends AttributeDescription> 
			selectedAttributes, 
			DatabaseTable fromTable, DatabaseTable joinTable, Collection<? extends 
			JoinCondition> joinConditions, WhereClause where, 
			int limit, String extraSQL) throws DatabaseException, DatabaseUnavailableException
	{
		List<List<Attribute>> rows = new ArrayList<>();
		
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(getColumnNameString(selectedAttributes));
		sql.append(" FROM ");
		sql.append(fromTable.getTableName());
		sql.append(getJoinSql(fromTable, joinTable, joinConditions));
		if (where != null)
			sql.append(where.toSql(fromTable));
		if (limit >= 0)
			sql.append(" LIMIT " + limit);
		if (extraSQL != null)
			sql.append(extraSQL);
		
		// Executes the query
		DatabaseAccessor accessor = new DatabaseAccessor(fromTable.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			// Prepares the statement
			statement = accessor.getPreparedStatement(sql.toString());
			if (where != null)
				where.setObjectValues(statement, 1);
			
			// Parses the results
			results = statement.executeQuery();
			while (results.next())
			{
				List<Attribute> attributes = new ArrayList<>();
				for (AttributeDescription description : selectedAttributes)
				{
					attributes.add(new Attribute(description, 
							results.getObject(description.getColumnName())));
				}
				rows.add(attributes);
			}
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), fromTable, where, null);
		}
		finally
		{
			closeResults(results);
			closeStatement(statement);
			accessor.closeConnection();
		}
		
		return rows;
	}
	
	/**
	 * Selects all data from certain rows from the table
	 * @param fromTable The table the selection is made from
	 * @param where The where clause used in the select (null if all rows are selected)
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @param extraSQL Extra sql code added to the end of the query. Can be, for example, 
	 * sorting information. Null if not used.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 * @throws NoAttributeForColumnException If a table's column name couldn't be mapped to an 
	 * attribute name
	 */
	public static List<List<Attribute>> selectAll(DatabaseTable fromTable, 
			WhereClause where, int limit, String extraSQL) 
			throws DatabaseUnavailableException, DatabaseException, NoAttributeForColumnException
	{
		return select(Attribute.getDescriptionsFrom(fromTable), fromTable, where, limit, extraSQL);
	}
	
	/**
	 * Selects all data from certain rows from the two joined tables
	 * @param fromTable The table the selection is made from
	 * @param joinTable The table that is joined into the selection
	 * @param joinConditions The conditions on which the rows are joined
	 * @param where The where clause used in the select (null if all rows are selected)
	 * @param limit How many rows are returned at maximum. If this is a negative number, 
	 * the number of returned rows is not limited.
	 * @param extraSQL Extra sql code added to the end of the query. Can be, for example, 
	 * sorting information. Null if not used.
	 * @return A list of attribute sets. One set for each row. Each set contains selected 
	 * data for that row.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 * @throws NoAttributeForColumnException If a table's column name couldn't be mapped to an 
	 * attribute name
	 */
	public static List<List<Attribute>> selectAll(DatabaseTable fromTable, 
			DatabaseTable joinTable, Collection<? extends JoinCondition> joinConditions, 
			WhereClause where, int limit, String extraSQL) 
			throws DatabaseUnavailableException, DatabaseException, NoAttributeForColumnException
	{
		List<AttributeDescription> selection = new ArrayList<>();
		selection.addAll(Attribute.getDescriptionsFrom(fromTable));
		if (joinTable != null)
			selection.addAll(Attribute.getDescriptionsFrom(joinTable));
		return select(selection, fromTable, joinTable, joinConditions, 
				where, limit, extraSQL);
	}
	
	/**
	 * Inserts new data into a table
	 * @param intoTable The table the data will be inserted into
	 * @param attributes The attribute data inserted into the table. The attributes should 
	 * contain all "not null" columns
	 * @return An auto-increment index if one was generated, -1 otherwise
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static int insert(DatabaseTable intoTable, Collection<? extends Attribute> attributes) throws 
			DatabaseUnavailableException, DatabaseException
	{
		// If there are no values to insert or no table, does nothing
		if (intoTable == null || attributes == null)
			return -1;
			
		// Only inserts non-null values that fit into the database and don't use auto-increment
		Collection<Attribute> insertedAttributes = Attribute.getTableAttributesFrom(
				getNonNullAttributes(attributes), intoTable);
		removeAutoIncrementAttributes(insertedAttributes, intoTable.getColumnInfo());
		if (insertedAttributes.isEmpty())
			return -1;
		
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(intoTable.getTableName());
		sql.append(" (" + getColumnNameString(Attribute.getDescriptionsFrom(insertedAttributes)));
		sql.append(") VALUES ");
		sql.append(getValueSetString(insertedAttributes.size()));
		
		DatabaseAccessor accessor = new DatabaseAccessor(intoTable.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			statement = accessor.getPreparedStatement(sql.toString(), 
					intoTable.usesAutoIncrementIndexing());
			
			// Inserts the values executes statement
			prepareAttributeValues(statement, insertedAttributes, 1);
			boolean resultsFound = statement.execute();
			
			// Finds the generated indices, if necessary
			if (intoTable.usesAutoIncrementIndexing())
			{
				results = statement.getGeneratedKeys();
				if (results.next())
				{
					return results.getInt(intoTable.getPrimaryColumn().getName());
				}
			}
			else if (resultsFound)
				results = statement.getResultSet();
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), intoTable, null, attributes);
		}
		finally
		{
			closeResults(results);
			closeStatement(statement);
			accessor.closeConnection();
		}
		
		return -1;
	}
	
	/**
	 * Deletes rows where the provided conditions are met
	 * @param fromTable The table the row(s) are deleted from
	 * @param where The where clause used in the delete. Null if all rows are to be deleted
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed
	 */
	public static void delete(DatabaseTable fromTable, WhereClause where) 
			throws DatabaseUnavailableException, DatabaseException
	{
		delete(fromTable, null, null, where);
	}
	
	/**
	 * Deletes rows where the provided conditions are met
	 * @param fromTable The table the row(s) are deleted from
	 * @param joinTable The table that is joined into the deletion (null if no join is made)
	 * @param joinConditions The conditions on which the rows are joined (null if no join 
	 * is made)
	 * @param where The where clause used in the delete. Null if all rows are to be deleted
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed
	 */
	public static void delete(DatabaseTable fromTable, DatabaseTable joinTable, 
			Collection<? extends JoinCondition> joinConditions, 
			WhereClause where) throws DatabaseUnavailableException, DatabaseException
	{
		StringBuilder sql = new StringBuilder("DELETE FROM ");
		sql.append(fromTable.getTableName());
		sql.append(getJoinSql(fromTable, joinTable, joinConditions));
		if (where != null)
			sql.append(where.toSql(fromTable));
		
		DatabaseAccessor accessor = new DatabaseAccessor(fromTable.getDatabaseName());
		PreparedStatement statement = null;
		try
		{
			// TODO: Continue
			// Prepares the statement
			statement = accessor.getPreparedStatement(sql.toString());
			if (where != null)
				prepareAttributeValues(statement, whereAttributes, 1);
			// Executes
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), fromTable, whereAttributes);
		}
		finally
		{
			closeStatement(statement);
			accessor.closeConnection();
		}
	}
	
	/**
	 * Updates a value in a column for certain rows
	 * @param table The table in which the update is done
	 * @param targetColumn The column which should contain the targetValue
	 * @param targetValue The value the column should have in order to be updated. No brackets 
	 * needed.
	 * @param changeColumn The column which is being updated
	 * @param newValue The new value given to the changeColumn. No brackets needed.
	 * @throws SQLException If the update fails
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @deprecated One should use {@link #update(DatabaseTable, Collection, Collection, boolean)} instead
	 */
	public static void update(DatabaseTable table, String targetColumn, 
			String targetValue, String changeColumn, String newValue) throws SQLException, 
			DatabaseUnavailableException
	{
		String[] changeColumns = {changeColumn};
		String[] newValues = {newValue};
		update(table, targetColumn, targetValue, changeColumns, newValues, false);
	}
	
	/**
	 * Updates a set of columns for certain rows
	 * @param table The table in which the update is done
	 * @param targetColumn The column which should contain the targetValue
	 * @param targetValue The value the column should have in order to be updated. No brackets 
	 * needed. Nulls are allowed.
	 * @param changeColumns The columns which are being updated
	 * @param newValues The new values given to the changeColumns. No brackets needed.
	 * @param skipNulls If there are null values, should those be not updated into the database
	 * @throws SQLException If the update fails
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @deprecated One should use {@link #update(DatabaseTable, Collection, Collection, boolean)} instead
	 */
	public static void update(DatabaseTable table, String targetColumn, String targetValue, 
			String[] changeColumns, String[] newValues, boolean skipNulls) throws 
			SQLException, DatabaseUnavailableException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		
		try
		{
			StringBuilder statementBuilder = new StringBuilder("UPDATE ");
			statementBuilder.append(table.getTableName());
			statementBuilder.append(" SET ");
			
			int valuesWritten = 0;
			for (int columnIndex = 0; columnIndex < changeColumns.length; columnIndex ++)
			{
				String newValue = newValues[columnIndex];
				if (newValue == null)
				{
					if (skipNulls)
						continue;
					else
						newValue = "null";
				}
				else
					newValue = "'" + newValue + "'";
				
				if (columnIndex != 0)
					statementBuilder.append(", ");
				statementBuilder.append(changeColumns[columnIndex] + " = " + newValue);
				valuesWritten ++;
			}
			
			// The statement is executed only if new data was written
			if (valuesWritten != 0)
			{
				statementBuilder.append(" WHERE " + targetColumn + " = '" + targetValue + "'");
				
				accessor.executeStatement(statementBuilder.toString());
			}
		}
		finally
		{
			accessor.closeConnection();
		}
	}
	
	/**
	 * Updates attributes into database where the conditions are met
	 * @param intoTable The table the values will be updated into
	 * @param setAttributes The attributes that will be updated into the table
	 * @param whereAttributes The conditions for a row that must be met for the update to be 
	 * made. If an empty set or null is provided, all records will be updated. 
	 * Attribute names don't matter.
	 * @param noNullUpdates Should null value updates be skipped entirely
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static void update(DatabaseTable intoTable, Collection<? extends Attribute> setAttributes, 
			Collection<? extends Attribute> whereAttributes, boolean noNullUpdates) 
			throws DatabaseUnavailableException, DatabaseException
	{
		update(intoTable, setAttributes, whereAttributes, noNullUpdates, null);
	}
	
	/**
	 * Updates attributes into database where the conditions are met
	 * @param intoTable The table the values will be updated into
	 * @param setAttributes The attributes that will be updated into the table
	 * @param whereAttributes The conditions for a row that must be met for the update to be 
	 * made. If an empty set or null is provided, all records will be updated. Attribute names don't matter.
	 * @param noNullUpdates Should null value updates be skipped entirely
	 * @param extraSQL The sql string that will be added to the end of the update statement. 
	 * Null if not used.
	 * @throws DatabaseUnavailableException If the database can't be accessed at this time
	 * @throws DatabaseException If the operation failed
	 */
	public static void update(DatabaseTable intoTable, Collection<? extends Attribute> setAttributes, 
			Collection<? extends Attribute> whereAttributes, boolean noNullUpdates, 
			String extraSQL) throws DatabaseUnavailableException, DatabaseException
	{
		if (intoTable == null || setAttributes == null)
			return;
		
		// Only attributes that belong to the table and don't use auto-increment are updated
		List<Attribute> updatedAttributes = new ArrayList<>();
		updatedAttributes.addAll(setAttributes);
		// Null attributes may also be filtered
		if (noNullUpdates)
			updatedAttributes = getNonNullAttributes(setAttributes);
		removeAutoIncrementAttributes(updatedAttributes, intoTable.getColumnInfo());
		
		if (updatedAttributes.isEmpty())
			return;
		
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(intoTable.getTableName());
		sql.append(" SET ");
		sql.append(getEqualsString(updatedAttributes, ", ", false));
		if (whereAttributes != null && !whereAttributes.isEmpty())
		{
			sql.append(" WHERE ");
			sql.append(getEqualsString(whereAttributes, " AND ", true));
		}
		if (extraSQL != null)
			sql.append(extraSQL);
		
		// Executes the update
		DatabaseAccessor accessor = new DatabaseAccessor(intoTable.getDatabaseName());
		PreparedStatement statement = null;
		try
		{
			statement = accessor.getPreparedStatement(sql.toString());
			int whereAttributeIndex = prepareAttributeValues(statement, updatedAttributes, 1);
			if (whereAttributes != null && !whereAttributes.isEmpty())
				prepareAttributeValues(statement, whereAttributes, whereAttributeIndex);
			
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			List<Attribute> usedAttributes = new ArrayList<>();
			if (setAttributes != null)
				usedAttributes.addAll(setAttributes);
			if (whereAttributes != null)
				usedAttributes.addAll(whereAttributes);
			throw new DatabaseException(e, sql.toString(), intoTable, usedAttributes);
		}
		finally
		{
			closeStatement(statement);
			accessor.closeConnection();
		}
	}
	
	/**
	 * Updates the model's current attributes with the database's values. New attributes 
	 * won't be created.
	 * @param model The object that will be updated
	 * @return Was any data read from the database
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws IndexAttributeRequiredException If the provided object doesn't use 
	 * auto-increment indexing and doesn't have an index attribute
	 * @throws DatabaseException If the operation failed
	 */
	public static <T extends DatabaseReadable & DatabaseWritable> boolean 
			updateExistingObjectAttributesFromDatabase(T model) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		// If the object uses auto-increment indexing and doesn't have an index, it can't be 
		// in the database
		if (modelUsesAutoIncrementButHasNoIndex(model))
			return false;
		
		List<AttributeDescription> descriptions = Attribute.getDescriptionsFrom(
				model.getAttributes());
		if (descriptions.isEmpty())
			return false;
		
		List<List<Attribute>> results = select(descriptions, model.getTable(), 
				getIndexAttributeOrFail(model).wrapIntoList(), 1);
		if (!results.isEmpty())
		{
			model.updateAttributes(results.get(0));
			return true;
		}
		else
			return false;
	}
	
	/**
	 * Reads the object's data from the database. Adds new attributes if needed.
	 * @param model The object that is is updated from the database
	 * @param whereAttributes The attributes that define which row is read. Attribute names don't matter.
	 * @return Was any data read from the database
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the operation failed
	 * @throws NoAttributeForColumnException If a table's column name couldn't be mapped to an 
	 * attribute name
	 */
	public static boolean readObjectAttributesFromDatabase(DatabaseReadable model, 
			Collection<? extends Attribute> whereAttributes) throws 
			DatabaseUnavailableException, DatabaseException, NoAttributeForColumnException
	{
		List<List<Attribute>> results = DatabaseAccessor.selectAll(model.getTable(), 
				whereAttributes, 1, null);
		if (!results.isEmpty())
		{
			model.updateAttributes(results.get(0));
			return true;
		}
		else
			return false;
	}
	
	/**
	 * Updates the object into database. If there is no previous data, makes an insert. 
	 * Otherwise updates existing data
	 * @param model The object that will be written into the database
	 * @param noNullUpdates In case previous data will be updated, should null values be 
	 * skipped (leaving possible previous data)
	 * @throws DatabaseUnavailableException If the database is not available
	 * @throws IndexAttributeRequiredException If the object doesn't use auto-increment 
	 * indexing and doesn't have an index attribute
	 * @throws DatabaseException If the operation failed
	 */
	public static void updateObjectToDatabase(DatabaseWritable model, boolean noNullUpdates) 
			throws DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		// Auto-increment indexing models with no previous index are inserted as new
		if (!objectIsInDatabase(model))
			insertObjectIntoDatabase(model);
		else
			overwriteObjectIntoDatabase(model, noNullUpdates);
	}
	
	/**
	 * Inserts an object into the database if there is no previous data for the object
	 * @param model The object that may be inserted
	 * @return Was the object inserted into the database
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws IndexAttributeRequiredException If the object doesn't use auto-increment 
	 * indexing and doesn't have an index attribute
	 * @throws DatabaseException If the operation failed
	 */
	public static boolean insertObjectToDatabaseIfNotExists(DatabaseWritable model) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		// Auto-increment indexing models with no previous index are inserted as new
		if (!objectIsInDatabase(model))
		{
			insertObjectIntoDatabase(model);
			return true;
		}
		
		return false;
	}
	
	/**
	 * Updates object's data in the database, but only if there is already previous data 
	 * in the database
	 * @param model The object that will be updated into database
	 * @param noNullUpdates Should null attributes be skipped, leaving previous data intact
	 * @return Was the object updated into the database
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws IndexAttributeRequiredException If the object doesn't have an index attribute
	 * @throws DatabaseException If the operation failed
	 */
	public static boolean updateObjectToDatabaseIfExists(DatabaseWritable model, 
			boolean noNullUpdates) throws DatabaseUnavailableException,  
			IndexAttributeRequiredException, DatabaseException
	{
		if (!objectIsInDatabase(model))
			return false;
		overwriteObjectIntoDatabase(model, noNullUpdates);
		return true;
	}
	
	/**
	 * Deletes the object from the database (deletes a row with the same primary key index)
	 * @param model The model that will be deleted
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws IndexAttributeRequiredException If the object doesn't have an index
	 * @throws DatabaseException If the operation failed
	 */
	public static void deleteObjectFromDatabase(DatabaseWritable model) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		delete(model.getTable(), getIndexAttributeOrFail(model).wrapIntoList());
	}
	
	/**
	 * Checks if there is already data where the object would be saved
	 * @param model The object that may exist in the database
	 * @return Is there already data where the object would be saved (on same primary key index)
	 * @throws DatabaseUnavailableException If the database is unavailable
	 * @throws IndexAttributeRequiredException If the object doesn't use auto-increment 
	 * indexing and doesn't have an index attribute
	 * @throws DatabaseException If the operation failed
	 */
	public static boolean objectIsInDatabase(DatabaseWritable model) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		if (modelUsesAutoIncrementButHasNoIndex(model))
			return false;
		
		// Finds the existing index, if there is one
		List<List<Attribute>> result = select(
				Attribute.getDescriptionsFrom(getIndexAttributeOrFail(model).wrapIntoList()), 
				model.getTable(), DatabaseWritable.getIndexAttribute(model).wrapIntoList(), 1);
		
		return !result.isEmpty();
	}
	
	private static void insertObjectIntoDatabase(DatabaseWritable model) throws 
			DatabaseUnavailableException, DatabaseException
	{
		int newIndex = insert(model.getTable(), model.getAttributes());
		// Informs the object if a new index was generated
		if (newIndex >= 0)
			model.newIndexGenerated(newIndex);
	}
	
	private static void overwriteObjectIntoDatabase(DatabaseWritable model, 
			boolean noNullUpdates) throws DatabaseUnavailableException, 
			IndexAttributeRequiredException, DatabaseException
	{
		update(model.getTable(), model.getAttributes(), 
				getIndexAttributeOrFail(model).wrapIntoList(), noNullUpdates);
	}
	
	private static List<Attribute> getNonNullAttributes(Collection<? extends Attribute> attributes)
	{
		List<Attribute> nonNull = new ArrayList<>();
		for (Attribute attribute : attributes)
		{
			if (!attribute.isNull())
				nonNull.add(attribute);
		}
		
		return nonNull;
	}
	
	private static String getColumnNameString(Collection<? extends AttributeDescription> attributes)
	{
		StringBuilder sql = new StringBuilder();
		
		boolean firstAttribute = true;
		for (AttributeDescription attribute : attributes)
		{
			if (!firstAttribute)
				sql.append(", ");
			sql.append(attribute.getColumnName());
			firstAttribute = false;
		}
		
		return sql.toString();
	}
	
	private static String getEqualsString(Collection<? extends Attribute> attributes, 
			String separator, boolean useIsWithNulls)
	{
		StringBuilder sql = new StringBuilder();
		boolean firstAttribute = true;
		for (Attribute attribute : attributes)
		{
			if (!firstAttribute)
				sql.append(separator);
			sql.append(attribute.getDescription().getColumnName());
			// With null values, 'is' is used instead of '='
			if (attribute.isNull() && useIsWithNulls)
				sql.append(" is ?");
			else
				sql.append(" = ?");
			firstAttribute = false;
		}
		
		return sql.toString();
	}
	
	private static String getValueSetString(int attributeAmount)
	{
		StringBuilder sql = new StringBuilder("(");
		for (int i = 0; i < attributeAmount; i++)
		{
			if (i != 0)
				sql.append(", ?");
			else
				sql.append("?");
		}
		sql.append(")");
		
		return sql.toString();
	}
	
	private static String getJoinSql(DatabaseTable firstTable, DatabaseTable joinTable, 
			Collection<? extends JoinCondition> joinConditions)
	{
		StringBuilder sql = new StringBuilder();
		if (joinTable != null)
		{
			sql.append(" JOIN ");
			sql.append(joinTable.getTableName());
			
			if (joinConditions != null && !joinConditions.isEmpty())
			{
				sql.append(" ON ");
				boolean isFirst = true;
				for (JoinCondition condition : joinConditions)
				{
					if (!isFirst)
						sql.append(" AND ");
					sql.append(condition.toSql(firstTable, joinTable));
					isFirst = false;
				}
			}
		}
		return sql.toString();
	}
	
	private static int prepareAttributeValues(PreparedStatement statement, 
			Collection<? extends Attribute> attributes, int startIndex) throws SQLException
	{
		int i = startIndex;
		for (Attribute attribute : attributes)
		{
			if (attribute.isNull())
				statement.setNull(i, attribute.getDescription().getType());
			else
				statement.setObject(i, attribute.getValue(), 
						attribute.getDescription().getType());
			i ++;
		}
		
		return i;
	}
	
	private static void removeAutoIncrementAttributes(
			Collection<? extends Attribute> targetGroup, 
			List<? extends ColumnInfo> columnInfo)
	{
		List<Attribute> autoIncrementAttributes = new ArrayList<>();
		for (Attribute attribute : targetGroup)
		{
			ColumnInfo matchingColumn = 
					attribute.findMatchingColumnFrom(columnInfo);
			if (matchingColumn.usesAutoIncrementIndexing())
				autoIncrementAttributes.add(attribute);
		}
		targetGroup.removeAll(autoIncrementAttributes);
	}
	
	private static Attribute getIndexAttributeOrFail(DatabaseWritable object) 
			throws IndexAttributeRequiredException
	{
		Attribute indexAttribute = DatabaseWritable.getIndexAttribute(object);
		if (indexAttribute == null)
			throw new IndexAttributeRequiredException(object);
		return indexAttribute;
	}
	
	private static boolean modelUsesAutoIncrementButHasNoIndex(DatabaseWritable model)
	{
		if (!model.getTable().usesAutoIncrementIndexing())
			return false;
		
		Attribute index = DatabaseWritable.getIndexAttribute(model);
		return (index == null || index.isNull());
	}
	
	/**
	 * Inserts new data into the given table while also collecting and returning the 
	 * auto-generated id. This should be used for auto-indexing tables.
	 * @param targetTable The table the data is inserted into
	 * @param columnData The data posted to the table (in the same order as in the tables). 
	 * Strings need to be surrounded with brackets (')
	 * @param idColumnName The name of the auto-increment id column
	 * @return The id generated during the insert
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws SQLException If the insert failed or if the table doesn't use auto-increment 
	 * indexing
	 * @throws InvalidTableTypeException If the table doesn't use auto-increment indexing
	 * @deprecated Not updated anymore
	 */
	private static int insertWithAutoIncrement(DatabaseTable targetTable, String columnData) 
			throws SQLException, DatabaseUnavailableException, InvalidTableTypeException
	{
		// Doesn't work if the table doesn't use auto-increment
		if (!targetTable.usesAutoIncrementIndexing())
			throw new InvalidTableTypeException(
					"No id can be retrieved since the table doesn't use auto-increment indexing");
		
		DatabaseAccessor accessor = new DatabaseAccessor(targetTable.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet autoKeys = null;
		
		try
		{
			statement = accessor.getPreparedStatement(getInsertStatement(targetTable, 
					columnData), true);
			statement.execute();
			
			autoKeys = statement.getGeneratedKeys();
			
			if (autoKeys.next())
			{
				return autoKeys.getInt(targetTable.getPrimaryColumn().getName());
				//DatabaseSettings.getTableHandler().updateTableAmount(targetTable, id);
				//return id;
			}
		}
		finally
		{
			DatabaseAccessor.closeResults(autoKeys);
			DatabaseAccessor.closeStatement(statement);
			accessor.closeConnection();
		}
		
		return 0;
	}
	
	private static String getColumnDataString(List<String> columnData)
	{
		StringBuilder columnDataString = new StringBuilder();
		
		for (int i = 0; i < columnData.size(); i++)
		{
			if (i != 0)
				columnDataString.append(", ");
			String nextData = columnData.get(i);
			if (nextData != null)
				columnDataString.append("'" + columnData.get(i) + "'");
			else
				columnDataString.append("null");
		}
		
		return columnDataString.toString();
	}
	
	private static String getInsertStatement(DatabaseTable table, String columnDataString)
	{
		StringBuilder statement = new StringBuilder();
		statement.append("INSERT INTO ");
		
		/*
		if (index < 0)
			statement.append(DatabaseSettings.getTableHandler().getLatestTableName(table));
		else
			statement.append(DatabaseSettings.getTableHandler().getTableNameForIndex(
					table, index, true));
		*/
		statement.append(table.getTableName());
		
		statement.append(" values (");
		
		if (table.usesAutoIncrementIndexing())
			statement.append("null, ");
			
		statement.append(columnDataString);
		
		statement.append(");");
		
		return statement.toString();
	}
	
	private static void checkPrimaryKeyColumn(DatabaseTable table) throws InvalidTableTypeException
	{
		if (table.getPrimaryColumn() == null)
			throw new InvalidTableTypeException(
					"table " + table + " doesn't have a primary key column");
	}
	
	
	// SUBCLASSES	----------------------
	
	/**
	 * JoinConditions are used for parsing sql join conditions
	 * @author Mikko Hilpinen
	 * @since 1.10.2015
	 */
	public static abstract class JoinCondition
	{
		// ABSTRACT METHODS	--------------
		
		/**
		 * This method returns the column name that is part of the condition
		 * @param table The table the column should be from
		 * @param index The index of the condition (0 | 1). 0 Is requested first, then 1.
		 * @return A column name for the given index / table.
		 */
		protected abstract String getColumnNameForTable(DatabaseTable table, int index);
		
		
		// OTHER METHODS	--------------
		
		/**
		 * Creates an sql equals statement from the condition. The sql statement will be 
		 * something like firstTable.columnName = secondTable.columnName
		 * @param firstTable The first table that is used
		 * @param joinedTable The table that is joined into the query
		 * @return The sql statement for the equality
		 */
		public String toSql(DatabaseTable firstTable, DatabaseTable joinedTable)
		{
			StringBuilder sql = new StringBuilder();
			sql.append(firstTable.getTableName() + ".");
			sql.append(getColumnNameForTable(firstTable, 0));
			sql.append(" = ");
			sql.append(joinedTable.getTableName() + ".");
			sql.append(getColumnNameForTable(joinedTable, 1));
			
			return sql.toString();
		}
		
		/**
		 * @return The joinCondition wrapped into a list of joinConditions. The list will be 
		 * of size 1.
		 */
		public List<JoinCondition> wrapIntoList()
		{
			List<JoinCondition> list = new ArrayList<>();
			list.add(this);
			return list;
		}
	}
	
	/**
	 * This joinCondition uses attribute descriptions
	 * @author Mikko Hilpinen
	 * @since 1.10.2015
	 */
	public static class DescriptionJoinCondition extends JoinCondition
	{
		// ATTRIBUTES	------------------
		
		private AttributeDescription[] descriptions;
		
		
		// CONSTRUCTOR	------------------
		
		/**
		 * Creates a new join condition. The values of the two attributes in the database must 
		 * be the same. The descriptions should describe columns in the joined tables.
		 * @param first The description of the attribute in the first table
		 * @param second The description of the attribute in the joined table
		 */
		public DescriptionJoinCondition(AttributeDescription first, 
				AttributeDescription second)
		{
			this.descriptions = new AttributeDescription[2];
			this.descriptions[0] = first;
			this.descriptions[1] = second;
		}
		
		
		// IMPLEMENTED METHODS	---------
		
		@Override
		public String getColumnNameForTable(DatabaseTable table, int index)
		{
			return this.descriptions[index].getColumnName();
		}
	}
	
	/**
	 * This joinCondition uses attribute names
	 * @author Mikko Hilpinen
	 * @since 1.10.2015
	 */
	public static class AttributeNameJoinCondition extends JoinCondition
	{
		// ATTRIBUTES	------------------
		
		private String[] names;
		
		
		// CONSTRUCTOR	------------------
		
		/**
		 * Creates a new join condition. The attribute names should be represented in the 
		 * tables.
		 * @param firstAttributeName The name of the attribute in the first table
		 * @param secondAttributeName The name of the attribute in the joined table
		 */
		public AttributeNameJoinCondition(String firstAttributeName, 
				String secondAttributeName)
		{
			this.names = new String[2];
			this.names[0] = firstAttributeName;
			this.names[1] = secondAttributeName;
		}
		
		
		// IMPLEMENTED METHODS	----------
		
		@Override
		public String getColumnNameForTable(DatabaseTable table, int index)
		{
			try
			{
				return table.getAttributeNameMapping().getColumnName(this.names[index]);
			}
			catch (NoColumnForAttributeException e)
			{
				System.err.println("Failed to find a column name in join condition");
				e.printStackTrace();
				return null;
			}
		}
	}
	
	/**
	 * This joinCondition uses column names
	 * @author Mikko Hilpinen
	 * @since 1.10.2015
	 */
	public static class SimpleJoinCondition extends JoinCondition
	{
		// ATTRIBUTES	---------------
		
		private String[] columnNames;
		
		/**
		 * Creates a new join condition. The column names should be present in the joined tables.
		 * @param firstColumnName The name of the column in the first table
		 * @param secondColumnName The name of the column in the joined table
		 */
		public SimpleJoinCondition(String firstColumnName, String secondColumnName)
		{
			this.columnNames = new String[2];
			this.columnNames[0] = firstColumnName;
			this.columnNames[1] = secondColumnName;
		}
		
		// IMPLEMENTED METHODS	--------
		
		@Override
		public String getColumnNameForTable(DatabaseTable table, int index)
		{
			return this.columnNames[index];
		}
	}
}
