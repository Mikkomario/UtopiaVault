package utopia.vault.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.Value;
import utopia.vault.database.ComparisonCondition.Operator;
import utopia.vault.database.Condition.ConditionParseException;
import utopia.vault.database.DatabaseSettings.UninitializedSettingsException;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.Table;
import utopia.vault.generics.TableModel;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * The database class is used as an interface for executing various sql queries over a database
 * @author Mikko Hilpinen
 * @since 17.1.2016
 */
public class Database
{
	// ATTRIBUTES	-----------------
	
	private String name;
	private Connection connection = null;
	
	
	// CONSTRUCTOR	-----------------
	
	/**
	 * Creates a new database interface
	 * @param name The name of the database
	 */
	public Database(String name)
	{
		this.name = name;
	}
	
	/**
	 * Creates a new database interface for the database the provided table uses
	 * @param table A table
	 */
	public Database(Table table)
	{
		this.name = table.getDatabaseName();
	}
	
	/**
	 * Creates a new database interface. The interface is not connected to any single database. 
	 * The database needs to be specified before the object can be used. This constructor 
	 * can be used with the static utility methods since they specify the database before 
	 * connecting.
	 * @see #switchTo(String)
	 */
	public Database()
	{
		this.name = null;
	}
	
	
	// ACCESSORS	-----------------
	
	/**
	 * @return The name of the database
	 */
	public String getName()
	{
		return this.name;
	}
	
	
	// OTHER METHODS	-------------
	
	/**
	 * Returns the connection used. Please note that the ownership of the connection stays 
	 * with this instance and will be closed when {@link #closeConnection()} is 
	 * called. Calling this method will cause the connection to be opened
	 * @return Connection An open connection used by this instance.
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws SQLException If the connection failed
	 */
	public Connection getOpenConnection() throws DatabaseUnavailableException, SQLException
	{
		if (this.connection == null)
			openConnection();
		else if (this.connection.isClosed())
		{
			this.connection = null;
			openConnection();
		}
		else if (!this.connection.isValid(10))
		{
			closeConnection();
			openConnection();				
		}
		
		return this.connection;
	}
	
	/**
	 * This method opens a connection to the database. The 
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
			if (this.connection != null && !this.connection.isClosed())
				return;
			
			String driver = DatabaseSettings.getDriver();
			if (driver != null)
			{
				try
				{
					Class.forName(driver).newInstance();
				}
				catch (Exception e)
				{
					throw new DatabaseUnavailableException("Can't use driver " + driver, e);
				}
			}
			this.connection = DriverManager.getConnection(
					DatabaseSettings.getConnectionTarget() + getName(), 
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
			if (this.connection == null || this.connection.isClosed())
				return;
			
			this.connection.close();
			this.connection = null;
		}
		catch (SQLException e)
		{
			System.err.println("Couldn't close connection to database " + getName());
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
			statement = getOpenConnection().createStatement();
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
	 * @param sqlStatement The sql statement that will be prepared. The 
	 * statement may include multiple '?' as place holders for future parameters.
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
			statement = getOpenConnection().prepareStatement(sqlStatement, 
					autoGeneratedKeys);
		}
		// Closes the statement if the operation fails
		catch (SQLException | DatabaseUnavailableException e)
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
	 * @param sqlStatement The sql statement that will be prepared. The 
	 * statement may include multiple '?' as place holders for future parameters.
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
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws SQLException If an sql exception occurred
	 */
	public String switchTo(String newDatabaseName) throws SQLException, 
			DatabaseUnavailableException
	{
		if (this.name == null || !this.name.equals(newDatabaseName))
		{
			// The change is simple when a connection is closed
			if (this.connection == null || this.connection.isClosed())
				this.name = newDatabaseName;
			// When a connection is open, informs the server
			else
			{
				executeStatement("USE " + newDatabaseName + ";");
				this.name = newDatabaseName;	
			}
		}
		
		return this.name;
	}
	
	/**
	 * Closes a currently open statement
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
			System.err.println("Failed to close a statement");
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes a currently open resultSet
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
			System.err.println("Failed to close a resultSet");
			e.printStackTrace();
		}
	}
	
	/**
	 * Performs a select query, selecting certain column value(s) from certain row(s) in certain 
	 * table(s)
	 * @param select The column values that are selected from each row. Null if the whole row 
	 * should be selected. Use an empty list if no variables should be selected.
	 * @param from The table the selection is made on
	 * @param joins The joins that are inserted to the query (optional)
	 * @param where The condition that specifies which rows are selected. Null if each row 
	 * should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. < 0 if no limit 
	 * should be set
	 * @param orderBy The method the returned rows are sorted with (optional)
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	@SuppressWarnings("resource")
	public static List<List<ColumnVariable>> select(Collection<? extends Column> select, 
			Table from, Join[] joins, Condition where, int limit, 
			OrderBy orderBy, Database connection) throws DatabaseUnavailableException, DatabaseException
	{
		List<List<ColumnVariable>> rows = new ArrayList<>();
		
		StringBuilder sql = new StringBuilder();
		appendSelect(sql, select);
		sql.append(" FROM ");
		sql.append(from.getName());
		if (joins != null)
			appendJoin(sql, joins);
		if (where != null)
		{
			try
			{
				sql.append(where.toWhereClause());
			}
			catch (ConditionParseException e)
			{
				throw new DatabaseException(e, where);
			}
		}
		if (orderBy != null)
			sql.append(orderBy.toSql());
		if (limit >= 0)
			sql.append(" LIMIT " + limit);
		
		// Executes the query
		Database db = null;
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			db = openIfTemporary(from, connection);
			
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			int index = 1;
			if (joins != null)
				index = setConditionValues(statement, index, joins);
			if (where != null)
				index = setConditionValues(statement, index, where);
			
			// Parses the results
			results = statement.executeQuery();
			while (results.next())
			{
				List<ColumnVariable> row = new ArrayList<>();
				// If it was select *, all rows from all tables are read
				List<Column> readColumns = new ArrayList<Column>();;
				if (select == null)
				{
					readColumns.addAll(from.getColumns());
					if (joins != null)
					{
						for (Join join : joins)
						{
							readColumns.addAll(join.getJoinedTable().getColumns());
						}
					}
				}
				else
					readColumns.addAll(select);
				
				for (Column column : readColumns)
				{
					row.add(column.assignValue(results.getObject(column.getColumnName())));
				}
				rows.add(row);
			}
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), from, where, null, select);
		}
		finally
		{
			closeResults(results);
			closeStatement(statement);
			closeIfTemporary(db, connection);
		}
		
		return rows;
	}
	
	/**
	 * Performs a select query, selecting certain column value(s) from certain row(s) in certain 
	 * table(s)
	 * @param select The column values that are selected from each row. Null if the whole row 
	 * should be selected. Use an empty list if no variables should be selected.
	 * @param from The table the selection is made on
	 * @param where The condition that specifies which rows are selected. Null if each row 
	 * should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. < 0 if no limit 
	 * should be set
	 * @param orderBy The method the returned rows are sorted with (optional)
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	public static List<List<ColumnVariable>> select(Collection<? extends Column> select, 
			Table from, Condition where, int limit, OrderBy orderBy, Database connection) 
			throws DatabaseUnavailableException, DatabaseException
	{
		return select(select, from, null, where, limit, orderBy, connection);
	}
	
	/**
	 * Updates a model's attributes based on a database query
	 * @param model The model who's attributes are updated
	 * @param where The condition with which the model is found. Notice that only the first 
	 * result will be read
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return Was any data read
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static boolean readModelAttributes(TableModel model, Condition where, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		// The index value is used as a where condition
		List<List<ColumnVariable>> result = select(null, model.getTable(), null, 
				where, 1, null, connection);
		if (result.isEmpty())
			return false;
		else
		{
			model.addAttributes(result.get(0), true);
			return true;
		}
	}
	
	/**
	 * Updates a model's attributes based on a database query
	 * @param model The model who's attributes are updated
	 * @param index The index with which the model is found from the database
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return Was any data read
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 */
	public static boolean readModelAttributes(TableModel model, Value index, Database connection) throws 
			DatabaseException, DatabaseUnavailableException, NoSuchColumnException
	{
		if (model == null || index == null)
			return false;
		
		// The index value is used as a where condition
		Condition where = ComparisonCondition.createIndexCondition(model, Operator.EQUALS);
		List<List<ColumnVariable>> result = select(null, model.getTable(), null, 
				where, 1, null, connection);
		if (result.isEmpty())
			return false;
		else
		{
			model.addAttributes(result.get(0), true);
			return true;
		}
	}

	/**
	 * Inserts new data into a database table
	 * @param insert The data that is inserted into the table
	 * @param into The table the data is inserted into
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return An auto-generated index, if there was one, -1 otherwise.
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed / was misused
	 */
	@SuppressWarnings("resource")
	public static int insert(Collection<? extends ColumnVariable> insert, Table into, 
			Database connection) throws 
			DatabaseUnavailableException, DatabaseException
	{
		// If there are no values to insert or no table, does nothing
		if (into == null || insert == null)
			return -1;
			
		// Only inserts non-null values that fit into the table and don't use auto-increment
		Collection<ColumnVariable> actualInsert = getManualVariables(getTableVariables(
				getNonNullVariables(insert), into));
		if (actualInsert.isEmpty())
			return -1;
		
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(into.getName());
		sql.append(" (");
		appendColumnNames(sql, Column.getColumnsFrom(actualInsert));
		sql.append(") VALUES ");
		appendValueSetString(sql, actualInsert.size());
		
		Database db = null;
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			db = openIfTemporary(into, connection);
			statement = db.getPreparedStatement(sql.toString(), 
					into.usesAutoIncrementIndexing());
			
			// Inserts the values executes statement
			setVariableValues(statement, actualInsert, 1);
			boolean resultsFound = statement.execute();
			
			// Finds the generated indices, if necessary
			if (into.usesAutoIncrementIndexing())
			{
				results = statement.getGeneratedKeys();
				if (results.next())
					return results.getInt(1);
			}
			else if (resultsFound)
				results = statement.getResultSet();
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), into, null, insert, null);
		}
		finally
		{
			closeResults(results);
			closeStatement(statement);
			closeIfTemporary(db, connection);
		}
		
		return -1;
	}
	
	/**
	 * Inserts a model into the database. The model should either have an existing index 
	 * attribute, or use a table with auto-increment indexing
	 * @param model A model
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void insert(TableModel model, Database connection) throws DatabaseException, 
	DatabaseUnavailableException
	{
		if (model != null)
		{
			int index = insert(model.getAttributes(), model.getTable(), connection);
			
			if (index >= 0)
				model.setIndex(Value.Integer(index));
		}
	}
	
	/**
	 * Inserts a model into the database, if the model already exists in the database, updates 
	 * it instead
	 * @param model a model
	 * @param skipNullUpdates Should null updates be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the query failed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void insertOrUpdate(TableModel model, boolean skipNullUpdates, 
			Database connection) throws 
			DatabaseException, NoSuchColumnException, DatabaseUnavailableException
	{
		// Uses a single connection for all operations
		Database db = null;
		try
		{
			db = openIfTemporary(model.getTable(), connection);
			if (modelExists(model, db))
				update(model, skipNullUpdates, db);
			else
				insert(model, db);
		}
		finally
		{
			closeIfTemporary(db, connection);
		}
	}
	
	/**
	 * Deletes row(s) from a database table
	 * @param from The table the row(s) are deleted from
	 * @param joins The joins that are performed in the query (optional)
	 * @param where The condition which determines, which rows are deleted
	 * @param deleteFromJoined Should the joined rows be deleted as well
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed / was misused
	 */
	@SuppressWarnings("resource")
	public static void delete(Table from, Join[] joins, Condition where, 
			boolean deleteFromJoined, Database connection) throws DatabaseUnavailableException, 
			DatabaseException
	{
		StringBuilder sql = new StringBuilder("DELETE ");
		// By default, only target table rows are deleted, joined rows can be included though
		sql.append(from.getName());
		if (deleteFromJoined && joins != null)
		{
			for (Join join : joins)
			{
				sql.append(", ");
				sql.append(join.getJoinedTable().getName());
			}
		}
		
		sql.append(" FROM ");
		sql.append(from.getName());
		
		if (joins != null)
			appendJoin(sql, joins);
		
		if (where != null)
		{
			try
			{
				sql.append(where.toWhereClause());
			}
			catch (ConditionParseException e)
			{
				throw new DatabaseException(e, where);
			}
		}
		
		Database db = null;
		PreparedStatement statement = null;
		try
		{
			db = openIfTemporary(from, connection);
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			int index = 1;
			if (joins != null)
				index = setConditionValues(statement, index, joins);
			if (where != null)
				index = setConditionValues(statement, index, where);
			
			// Executes
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), from, where, null, null);
		}
		finally
		{
			closeStatement(statement);
			closeIfTemporary(db, connection);
		}
	}
	
	/**
	 * Deletes row(s) from a database table
	 * @param from The table the row(s) are deleted from
	 * @param where The condition which determines, which rows are deleted
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed / was misused
	 */
	public static void delete(Table from, Condition where, Database connection) 
			throws DatabaseUnavailableException, DatabaseException
	{
		delete(from, null, where, false, connection);
	}
	
	/**
	 * Updates certain row(s) in the provided table
	 * @param table The table that is updated
	 * @param set The variable values that are set
	 * @param where The condition which determines which rows are updated
	 * @param skipNullUpdates Should null updates be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	@SuppressWarnings("resource")
	public static void update(Table table, Collection<? extends ColumnVariable> set, 
			Condition where, boolean skipNullUpdates, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// TODO: Add support for join when necessary
		// UPDATE table1, table2 SET ... WHERE ...
		
		// Only updates attributes that belong to the target table and don't use auto-increment 
		// indexing
		List<ColumnVariable> actualSet = getManualVariables(getTableVariables(set, table));
		// Null updates may also be skipped
		if (skipNullUpdates)
			actualSet = getNonNullVariables(actualSet);
		
		if (actualSet.isEmpty())
			return;
		
		// Parses the sql
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(table.getName());
		
		appendSet(sql, actualSet);
		
		if (where != null)
		{
			try
			{
				sql.append(where.toWhereClause());
			}
			catch (ConditionParseException e)
			{
				throw new DatabaseException(e, where);
			}
		}
		
		// Prepares the query
		Database db = null;
		PreparedStatement statement = null;
		try
		{
			db = openIfTemporary(table, connection);
			statement = db.getPreparedStatement(sql.toString());
			
			// Prepares the values
			int index = setVariableValues(statement, actualSet, 1);
			if (where != null)
				index = setConditionValues(statement, index, where);
			
			// Executes the update
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			throw new DatabaseException(e, sql.toString(), table, where, set, null);
		}
		finally
		{
			closeStatement(statement);
			closeIfTemporary(db, connection);
		}
	}
	
	/**
	 * Updates a models data into the database
	 * @param model The model who's attributes are written into the database
	 * @param where The condition with which the updated rows are selected
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, Condition where, boolean skipNullUpdates, 
			Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		update(model.getTable(), model.getAttributes(), where, skipNullUpdates, connection);
	}
	
	/**
	 * Updates a models data into the database
	 * @param model The model who's attributes are written into the database
	 * @param index The index of the row that is updated
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the process failed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, Value index, boolean skipNullUpdates, 
			Database connection) throws DatabaseException, NoSuchColumnException, 
			DatabaseUnavailableException
	{
		update(model, new ComparisonCondition(model.getTable().getPrimaryColumn(), index), 
				skipNullUpdates, connection);
	}
	
	/**
	 * Updates a models data into the database. The updated row is selected with the model's 
	 * index
	 * @param model The model who's attributes are written into the database
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the process failed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, boolean skipNullUpdates, Database connection) 
			throws DatabaseException, DatabaseUnavailableException, NoSuchColumnException
	{
		update(model, ComparisonCondition.createIndexEqualsCondition(model), 
				skipNullUpdates, connection);
	}
	
	/**
	 * Checks whether the provided index exists in the table
	 * @param table A table
	 * @param index An index
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return Does the index already exist in the table
	 * @throws DatabaseException If the query failed
	 * @throws NoSuchColumnException If the table doesn't have a primary column
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static boolean indexExists(Table table, Value index, Database connection) throws 
			DatabaseException, NoSuchColumnException, DatabaseUnavailableException
	{
		List<List<ColumnVariable>> results = select(new ArrayList<>(), table, null, 
				new ComparisonCondition(table.getPrimaryColumn(), index), 1, null, connection);
		
		return !results.isEmpty();
	}
	
	/**
	 * Checks whether a model exists in the database
	 * @param model A model
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return does the model have a specified row in the database. Models with no index can't 
	 * exist in the database. Always returns false for tables with no primary key.
	 * @throws DatabaseException If the query failed.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed.
	 */
	public static boolean modelExists(TableModel model, Database connection) throws DatabaseException, 
			DatabaseUnavailableException
	{
		// If the model doesn't have an index attribute, it doesn't exist in the table yet
		if (!model.hasIndex())
			return false;
		
		// Otherwise checks whether the model's index exists in the table
		return indexExists(model.getTable(), model.getIndex(), connection);
	}
	
	// 
	private static Database openIfTemporary(Table targetTable, Database providedConnection) throws 
			DatabaseUnavailableException
	{
		// If no connection was provided, a new temporary connection is used
		if (providedConnection == null)
			return new Database(targetTable);
		else
		{
			try
			{
				providedConnection.switchTo(targetTable.getDatabaseName());
			}
			catch (SQLException e)
			{
				throw new DatabaseUnavailableException(e);
			}
			return providedConnection;
		}
	}
	
	private static void closeIfTemporary(Database usedConnection, Database providedConnection)
	{
		// The connection is only closed if it was temporary (= not provided)
		if (providedConnection == null)
			usedConnection.closeConnection();
	}
	
	private static void appendSelect(StringBuilder sql, Collection<? extends Column> selection)
	{
		sql.append("SELECT ");
		if (selection == null)
			sql.append("*");
		else if (selection.isEmpty())
			sql.append("NULL");
		else
			appendColumnNames(sql, selection);
	}
	
	private static void appendSet(StringBuilder sql, Collection<? extends ColumnVariable> set)
	{
		sql.append(" SET ");
		boolean first = true;
		for (ColumnVariable var : set)
		{
			if (!first)
				sql.append(", ");
			sql.append(var.getColumn().getColumnName());
			sql.append(" = ?");
			first = false;
		}
	}
	
	private static void appendColumnNames(StringBuilder sql, Collection<? extends Column> columns)
	{
		boolean first = true;
		for (Column column : columns)
		{
			if (!first)
				sql.append(", ");
			sql.append(column.getColumnNameWithTable());
			first = false;
		}
	}
	
	private static void appendValueSetString(StringBuilder sql, int valueAmount)
	{
		sql.append("(");
		for (int i = 0; i < valueAmount; i++)
		{
			if (i != 0)
				sql.append(", ?");
			else
				sql.append("?");
		}
		sql.append(")");
	}
	
	private static void appendJoin(StringBuilder sql, Join[] joins) throws DatabaseException
	{
		for (Join join : joins)
		{
			try
			{
				sql.append(join.toSql());
			}
			catch (ConditionParseException e)
			{
				throw new DatabaseException(e, join.getJoinCondition());
			}
		}
	}
	
	private static int setConditionValues(PreparedStatement statement, int startIndex, 
			Condition... conditions) throws DatabaseException, SQLException
	{
		int index = startIndex;
		for (Condition condition : conditions)
		{
			try
			{
				index = condition.setObjectValues(statement, index);
			}
			catch (ConditionParseException e)
			{
				throw new DatabaseException(e, condition);
			}
		}
		return index;
	}
	

	private static int setConditionValues(PreparedStatement statement, int startIndex, 
			Join[] joins) throws DatabaseException, SQLException
	{
		Condition[] conditions = new Condition[joins.length];
		for (int i = 0; i < joins.length; i++)
		{
			conditions[i] = joins[i].getJoinCondition();
		}
		
		return setConditionValues(statement, startIndex, conditions);
	}
	
	private static List<ColumnVariable> getNonNullVariables(
			Collection<? extends ColumnVariable> variables)
	{
		List<ColumnVariable> nonNull = new ArrayList<>();
		for (ColumnVariable variable : variables)
		{
			if (!variable.isNull())
				nonNull.add(variable);
		}
		
		return nonNull;
	}
	
	private static List<ColumnVariable> getTableVariables(
			Collection<? extends ColumnVariable> variables, Table table)
	{
		List<ColumnVariable> tableVars = new ArrayList<>();
		for (ColumnVariable variable : variables)
		{
			if (variable.getColumn().getTable().equals(table))
				tableVars.add(variable);
		}
		
		return tableVars;
	}
	
	private static List<ColumnVariable> getManualVariables(
			Collection<? extends ColumnVariable> variables)
	{
		List<ColumnVariable> notAuto = new ArrayList<>();
		for (ColumnVariable variable : variables)
		{
			if (!variable.getColumn().usesAutoIncrementIndexing())
				notAuto.add(variable);
		}
		
		return notAuto;
	}
	
	private static int setVariableValues(PreparedStatement statement, 
			Collection<? extends ColumnVariable> variables, int startIndex) throws SQLException
	{
		int index = startIndex;
		for (ColumnVariable variable : variables)
		{
			statement.setObject(index, variable.getValue().getObjectValue(), 
					variable.getColumn().getSqlType().getSqlType());
			index ++;
		}
		
		return index;
	}
}