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

import utopia.flow.generics.Value;
import vault_database.ComparisonCondition.Operator;
import vault_database.Condition.ConditionParseException;
import vault_database.DatabaseSettings.UninitializedSettingsException;
import vault_database_old.DatabaseUnavailableException;
import vault_generics.Column;
import vault_generics.ColumnVariable;
import vault_generics.Table;
import vault_generics.Table.NoSuchColumnException;
import vault_generics.TableModel;

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
	 * should be selected.
	 * @param from The table the selection is made on
	 * @param join The tables that are joined to the selection (optional)
	 * @param on The conditions on which the tables are joined (optional). There should be 
	 * a condition for each joined table.
	 * @param where The condition that specifies which rows are selected. Null if each row 
	 * should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. < 0 if no limit 
	 * should be set
	 * @param orderBy The column the values should be ordered by
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	@SuppressWarnings("resource")
	public static List<List<ColumnVariable>> select(Collection<? extends Column> select, 
			Table from, Table[] join, Condition[] on, Condition where, int limit, 
			Column orderBy) throws DatabaseUnavailableException, DatabaseException
	{
		List<List<ColumnVariable>> rows = new ArrayList<>();
		
		StringBuilder sql = new StringBuilder();
		appendSelect(sql, select);
		sql.append(" FROM ");
		sql.append(from.getName());
		if (join != null)
			appendJoin(sql, join, on);
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
		{
			sql.append(" ORDER BY ");
			sql.append(orderBy.getColumnNameWithTable());
		}
		if (limit >= 0)
			sql.append(" LIMIT " + limit);
		
		// Executes the query
		Database db = new Database(from);
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			int index = 1;
			if (on != null)
				index = setConditionValues(statement, index, on);
			if (where != null)
				index = setConditionValues(statement, index, where);
			
			// Parses the results
			results = statement.executeQuery();
			while (results.next())
			{
				List<ColumnVariable> row = new ArrayList<>();
				for (Column column : select)
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
			db.closeConnection();
		}
		
		return rows;
	}
	
	/**
	 * Updates a model's attributes based on a database query
	 * @param model The model who's attributes are updated
	 * @param where The condition with which the model is found. Notice that only the first 
	 * result will be read
	 * @return Was any data read
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static boolean readModelAttributes(TableModel model, Condition where) throws 
			DatabaseException, DatabaseUnavailableException
	{
		// The index value is used as a where condition
		List<List<ColumnVariable>> result = select(null, model.getTable(), null, null, 
				where, 1, null);
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
	 * @return Was any data read
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 */
	public static boolean readModelAttributes(TableModel model, Value index) throws 
			DatabaseException, DatabaseUnavailableException, NoSuchColumnException
	{
		if (model == null || index == null)
			return false;
		
		// The index value is used as a where condition
		Condition where = ComparisonCondition.createIndexCondition(model, Operator.EQUALS);
		List<List<ColumnVariable>> result = select(null, model.getTable(), null, null, 
				where, 1, null);
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
	 * @return An auto-generated index, if there was one, -1 otherwise.
	 * @throws DatabaseUnavailableException If the database can't be accessed
	 * @throws DatabaseException If the operation failed / was misused
	 */
	@SuppressWarnings("resource")
	public static int insert(Collection<? extends ColumnVariable> insert, Table into) throws 
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
		
		Database db = new Database(into);
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
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
			db.closeConnection();
		}
		
		return -1;
	}
	
	/**
	 * Deletes row(s) from a database table
	 * @param from The table the row(s) are deleted from
	 * @param join The tables that are joined into the query
	 * @param on The conditions on which the tables are joined. One condition should be 
	 * presented for each joined table.
	 * @param where The condition which determines, which rows are deleted
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed / was misused
	 */
	@SuppressWarnings("resource")
	public static void delete(Table from, Table[] join, Condition[] on, Condition where) 
			throws DatabaseUnavailableException, DatabaseException
	{
		StringBuilder sql = new StringBuilder("DELETE ");// FROM ");
		sql.append(from.getName());
		sql.append(" FROM ");
		sql.append(from.getName());
		
		if (join != null)
			appendJoin(sql, join, on);
		
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
		
		Database db = new Database(from);
		PreparedStatement statement = null;
		try
		{
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			int index = 1;
			if (on != null)
				index = setConditionValues(statement, index, on);
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
			db.closeConnection();
		}
	}
	
	/**
	 * Updates certain row(s) in the provided table
	 * @param table The table that is updated
	 * @param set The variable values that are set
	 * @param where
	 * @param skipNullUpdates
	 * @throws DatabaseException
	 * @throws DatabaseUnavailableException
	 */
	@SuppressWarnings("resource")
	public static void update(Table table, Collection<? extends ColumnVariable> set, 
			Condition where, boolean skipNullUpdates) throws DatabaseException, DatabaseUnavailableException
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
		Database db = new Database(table);
		PreparedStatement statement = null;
		try
		{
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
			db.closeConnection();
		}
	}
	
	/**
	 * Updates a models data into the database
	 * @param model The model who's attributes are written into the database
	 * @param where The condition with which the updated rows are selected
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, Condition where, boolean skipNullUpdates) throws 
			DatabaseException, DatabaseUnavailableException
	{
		update(model.getTable(), model.getAttributes(), where, skipNullUpdates);
	}
	
	/**
	 * Updates a models data into the database
	 * @param model The model who's attributes are written into the database
	 * @param index The index of the row that is updated
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @throws DatabaseException If the process failed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, Value index, boolean skipNullUpdates) throws 
			DatabaseException, NoSuchColumnException, DatabaseUnavailableException
	{
		update(model, new ComparisonCondition(model.getTable().getPrimaryColumn(), index), 
				skipNullUpdates);
	}
	
	/**
	 * Updates a models data into the database. The updated row is selected with the model's 
	 * index
	 * @param model The model who's attributes are written into the database
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @throws DatabaseException If the process failed
	 * @throws NoSuchColumnException If the model's table doesn't have a primary key
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, boolean skipNullUpdates) throws DatabaseException, 
			DatabaseUnavailableException, NoSuchColumnException
	{
		update(model, ComparisonCondition.createIndexEqualsCondition(model), 
				skipNullUpdates);
	}
	
	private static void appendSelect(StringBuilder sql, Collection<? extends Column> selection)
	{
		sql.append("SELECT ");
		if (selection == null)
			sql.append("*");
		else
			appendColumnNames(sql, selection);
	}
	
	private static void appendSet(StringBuilder sql, Collection<? extends ColumnVariable> set)
	{
		sql.append(" SET ");
		boolean first = false;
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
		boolean first = false;
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
	
	private static void appendJoin(StringBuilder sql, Table[] join, Condition[] on) throws 
			DatabaseException
	{
		for (int i = 0; i < join.length; i++)
		{
			sql.append(" JOIN ");
			sql.append(join[i]);
			if (on != null && i < on.length)
			{
				sql.append(" ON ");
				try
				{
					sql.append(on[i].toSql());
				}
				catch (ConditionParseException e)
				{
					throw new DatabaseException(e, on[i]);
				}
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
