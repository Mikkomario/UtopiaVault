package utopia.vault.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import utopia.flow.async.Volatile;
import utopia.flow.generics.DataType;
import utopia.flow.generics.DataTypeException;
import utopia.flow.generics.SubTypeSet;
import utopia.flow.generics.Value;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.Option;
import utopia.flow.structure.Pair;
import utopia.flow.structure.Try;
import utopia.vault.generics.Column;
import utopia.vault.generics.ColumnVariable;
import utopia.vault.generics.SqlDataType;
import utopia.vault.generics.Table;
import utopia.vault.generics.TableModel;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * The database class is used as an interface for executing various sql queries over a database
 * @author Mikko Hilpinen
 * @since 17.1.2016
 */
public class Database implements AutoCloseable
{
	// ATTRIBUTES	-----------------
	
	private String name;
	private Volatile<Option<Connection>> connection = new Volatile<>(Option.none());
	
	
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
	
	
	// IMPLEMENTED METHODS	---------
	
	@Override
	public void close()
	{
		closeConnection();
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
	 */
	@SuppressWarnings("resource")
	public Connection getOpenConnection() throws DatabaseUnavailableException
	{
		return connection.<Try<Connection>>pop(old -> 
		{
			try
			{
				if (old.isDefined() && !old.get().isClosed())
					return new Pair<>(Try.success(old.get()), old);
				else
				{
					Connection newConnection = openConnection();
					return new Pair<>(Try.success(newConnection), Option.some(newConnection));
				}
			}
			catch (Exception e)
			{
				return new Pair<>(Try.failure(e), Option.none());
			}
			
		}).unwrapThrowing(e -> new DatabaseUnavailableException(e));
	}
	
	/**
	 * This method opens a connection to the database. The 
	 * connection must be closed with {@link #closeConnection()} after the necessary 
	 * statements have been executed.
	 * @throws DatabaseUnavailableException If the connection couldn't be opened
	 * @throws SQLException If the operation failed
	 * @see #closeConnection()
	 */
	private Connection openConnection() throws DatabaseUnavailableException
	{
		// Tries to form a connection to the database
		try
		{
			Option<String> driver = DatabaseSettings.getDriver();
			if (driver.isDefined())
			{
				try
				{
					// Was previously Class.forName(...).newInstance();
					Class.forName(driver.get()).getDeclaredConstructor().newInstance();
				}
				catch (Exception e)
				{
					throw new DatabaseUnavailableException("Can't use driver " + driver, e);
				}
			}
			
			return DriverManager.getConnection(
					DatabaseSettings.getConnectionTarget() + getName(), 
					DatabaseSettings.getUser(), DatabaseSettings.getPassword().getValue());
		}
		catch (SQLException e)
		{
			throw new DatabaseUnavailableException(e);
		}
	}
	
	/*
	private void openConnection() throws DatabaseUnavailableException
	{
		// Tries to form a connection to the database
		try
		{
			// If a connection is already open, quits
			if (this.connection != null && !this.connection.isClosed())
				return;
			
			Option<String> driver = DatabaseSettings.getDriver();
			if (driver.isDefined())
			{
				try
				{
					// Was previously Class.forName(...).newInstance();
					Class.forName(driver.get()).getDeclaredConstructor().newInstance();
				}
				catch (Exception e)
				{
					throw new DatabaseUnavailableException("Can't use driver " + driver, e);
				}
			}
			this.connection = DriverManager.getConnection(
					DatabaseSettings.getConnectionTarget() + getName(), 
					DatabaseSettings.getUser(), DatabaseSettings.getPassword().getValue());
		}
		catch (SQLException e)
		{
			throw new DatabaseUnavailableException(e);
		}
	}*/
	
	/**
	 * Closes a currently open connection to the database.
	 */
	public void closeConnection()
	{
		connection.update(con -> 
		{
			con.forEach(c -> 
			{
				try
				{
					if (!c.isClosed())
						c.close();
				}
				catch (Exception e)
				{
					System.err.println("Couldn't close connection to database " + getName());
					e.printStackTrace();
				}
			});
			return Option.none();
		});
		
		// Closes the connection
		/*
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
		}*/
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
			if (connection.get().forAll(c -> Try.run(() -> c.isClosed()).success().getOrElse(true)))
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
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param joins The joins that are inserted to the query (optional)
	 * @param where The condition that specifies which rows are selected. None if all rows should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. None if no limit 
	 * should be set
	 * @param orderBy The method the returned rows are sorted with (optional)
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	public static ImmutableList<ImmutableList<ColumnVariable>> select(Selection select, 
			Table from, ImmutableList<Join> joins, Option<Condition> where, Option<Integer> limit, 
			Option<OrderBy> orderBy, Database connection) 
					throws DatabaseUnavailableException, DatabaseException
	{
		return select(select, from, joins, where, limit, Option.none(), orderBy, connection);
	}
	
	/**
	 * Performs a select query, selecting certain column value(s) from certain row(s) in certain 
	 * table(s)
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param joins The joins that are inserted to the query (optional)
	 * @param where The condition that specifies which rows are selected. None if all rows should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. None if no limit 
	 * should be set
	 * @param offset Amount of rows dropped from the result's beginning. None if all rows should 
	 * be returned. If specified, limit must also be present.
	 * @param orderBy The method the returned rows are sorted with (optional)
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	@SuppressWarnings("resource")
	public static ImmutableList<ImmutableList<ColumnVariable>> select(Selection select, 
			Table from, ImmutableList<Join> joins, Option<Condition> where, Option<Integer> limit, 
			Option<Integer> offset, Option<OrderBy> orderBy, Database connection) 
					throws DatabaseUnavailableException, DatabaseException
	{
		StringBuilder sql = new StringBuilder();
		appendSelect(sql, select);
		sql.append(" FROM ");
		sql.append(from.getName());
		if (joins != null)
			appendJoin(sql, joins);
		if (where != null && where.isDefined())
		{
			try
			{
				sql.append(where.get().toWhereClause());
			}
			catch (StatementParseException e)
			{
				throw new DatabaseException(e, where.get());
			}
		}
		if (orderBy != null && orderBy.isDefined())
			sql.append(orderBy.get().toSql());
		if (limit != null)
			limit.forEach(l -> sql.append(" LIMIT " + l));
		offset.forEach(o -> sql.append(" OFFSET " + o));
		
		Database db = null;
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			db = openIfTemporary(from, connection);
			
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			setStatementValues(statement, joins, where.toList());
			
			// Executes the query
			results = statement.executeQuery();
			
			// Parses the results
			ResultSetMetaData meta = results.getMetaData();
			
			// Determines which columns are read
			// If it was select *, all rows from all tables are read
			ImmutableList<Column> readColumns = select == null || select.selectsAll() ? 
					from.getColumns().plus(joins.flatMap(join -> join.getJoinedTable().getColumns())) : select.getColumns();
			
			// Matches the columns to the result indices. Also reads the data types
			int columnCount = meta.getColumnCount();
			List<Option<Column>> rowColumnsBuffer = new ArrayList<>(columnCount);
			List<Option<? extends DataType>> columnTypesBuffer = new ArrayList<>(columnCount);
			for (int i = 0; i < columnCount; i++)
			{
				Option<? extends DataType> type = SqlDataType.getDataType(meta.getColumnType(i + 1));
				Option<Column> matchingColumn = Option.none();
				
				// Finds the column matching the column name
				String columnName = meta.getColumnName(i + 1);
				for (Column column : readColumns)
				{
					if (column.getColumnName().equalsIgnoreCase(columnName))
					{
						// Makes sure the correct table column is used, in case there are 
						// columns with similar names in a join
						if (column.getTable().getName().equalsIgnoreCase(meta.getTableName(i + 1)))
						{
							// Remembers the column and makes sure data type is defined
							// If the matching column is not found, it won't be assigned
							matchingColumn = Option.some(column);
							if (type.isEmpty())
								type = Option.some(matchingColumn.get().getType());
							break;
						}
					}
				}
				
				rowColumnsBuffer.add(matchingColumn);
				columnTypesBuffer.add(type);
			}
			
			ImmutableList<Option<Column>> rowColumns = ImmutableList.of(rowColumnsBuffer);
			ImmutableList<Option<? extends DataType>> columnTypes = ImmutableList.of(columnTypesBuffer);
			
			// Reads the data
			List<ImmutableList<ColumnVariable>> rows = new ArrayList<>();
			while (results.next())
			{
				// Assigns column values to each row
				List<ColumnVariable> row = new ArrayList<>();
				for (int i = 0; i < rowColumns.size(); i++)
				{
					Option<Column> column = rowColumns.get(i);
					Option<? extends DataType> type = columnTypes.get(i);
					
					if (column.isDefined() && type.isDefined())
						row.add(column.get().assignValue(new Value(results.getObject(i + 1), type.get())));
				}
				rows.add(ImmutableList.of(row));
			}
			
			return ImmutableList.of(rows);
		}
		catch (SQLException | ValueInsertFailedException e)
		{
			throw new DatabaseException(e, sql.toString(), from, where, null, select);
		}
		finally
		{
			closeResults(results);
			closeStatement(statement);
			closeIfTemporary(db, connection);
		}
	}
	
	/**
	 * Performs a select query, selecting certain column value(s) from certain row(s) in certain 
	 * table(s)
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param where The condition that specifies which rows are selected. None if each row 
	 * should be selected.
	 * @param limit The limit on how many rows should be selected at maximum. None if no limit 
	 * should be set
	 * @param orderBy The method the returned rows are sorted with (optional)
	 * @param connection A database connection that should be used in the query. None if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	public static ImmutableList<ImmutableList<ColumnVariable>> select(Selection select, 
			Table from, Option<Condition> where, Option<Integer> limit, Option<OrderBy> orderBy, Database connection) 
			throws DatabaseUnavailableException, DatabaseException
	{
		return select(select, from, ImmutableList.empty(), where, limit, orderBy, connection);
	}
	
	/**
	 * Performs a select query, selecting certain column value(s) from certain row(s) in a certain 
	 * table
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param where The condition that specifies which rows are selected.
	 * @param connection A database connection that should be used in the query. None if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @return A list containing each selected row. Each row contains the selected column 
	 * values.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed
	 */
	public static ImmutableList<ImmutableList<ColumnVariable>> select(Selection select, Table from, Condition where, 
			Database connection) throws DatabaseUnavailableException, DatabaseException
	{
		return select(select, from, ImmutableList.empty(), Option.some(where), Option.none(), Option.none(), connection);
	}
	
	/**
	 * Selects a single row
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param where The condition that specifies the rows selected
	 * @param order The order which determines the first row. None if default order (row id).
	 * @param connection The database connection used
	 * @return The first row accepted by the condition. None if no such row exists
	 * @throws DatabaseException If query failed
	 * @throws DatabaseUnavailableException If database couldn't be accessed
	 */
	public static Option<ImmutableList<ColumnVariable>> selectSingle(Selection select, Table from, Condition where, 
			Option<OrderBy> order, Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		return select(select, from, new Option<>(where), Option.some(1), order, connection).headOption();
	}
	
	/**
	 * Selects a single value from a single row
	 * @param from The table the value is read from
	 * @param varName The name of the variable which is read
	 * @param where The condition used for finding the row
	 * @param order The ordering used (optional)
	 * @param connection The database connection used
	 * @return The read value. Empty value if no such row existed.
	 * @throws DatabaseException If query failed
	 * @throws DatabaseUnavailableException If database couldn't be accessed
	 */
	public static Value selectSingleValue(Table from, String varName, Condition where, Option<OrderBy> order, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		return selectSingle(new Selection(from, varName), from, where, order, connection).map(row -> 
				row.head().getValue()).getOrElse(() -> Value.NullValue(from.getColumnWithVariableName(varName).getType()));
	}
	
	/**
	 * Selects a single row based on row index
	 * @param select The selected columns
	 * @param from The table the selection is made on
	 * @param index The index that is selected
	 * @param connection The database connection used
	 * @return A row matching the index or none if no such index exists
	 * @throws DatabaseException Id query failed
	 * @throws NoSuchColumnException If table has no index
	 * @throws DatabaseUnavailableException If database couldn't be accessed
	 */
	public static Option<ImmutableList<ColumnVariable>> selectIndex(Selection select, Table from, Value index, 
			Database connection) throws DatabaseException, NoSuchColumnException, DatabaseUnavailableException
	{
		return selectSingle(select, from, ComparisonCondition.createIndexEqualsCondition(from, index), Option.none(), 
				connection);
	}
	
	/**
	 * Fills a new model with new data and returns the model
	 * @param model An (empty) model
	 * @param where A condition with which the model is found
	 * @param connection A database connection used
	 * @return The updated model or none if data couldn't be read
	 * @throws DatabaseException If query failed
	 * @throws DatabaseUnavailableException If database couldn't be accessed
	 */
	public static <T extends TableModel> Option<T> selectModel(T model, Condition where, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		if (readModelAttributes(model, where, connection))
			return Option.some(model);
		else
			return Option.none();
	}
	
	/**
	 * Finds index for the first row that matches specified condition
	 * @param table Searched table
	 * @param where Search condition
	 * @param connection DB Connection
	 * @return Result index value. May be empty.
	 * @throws DatabaseException
	 * @throws DatabaseUnavailableException
	 */
	public static Value getIndex(Table table, Condition where, Database connection) 
			throws DatabaseException, DatabaseUnavailableException
	{
		return select(Selection.indexFrom(table), table, Option.some(where), Option.some(1), 
				Option.none(), connection).headOption()
				.flatMap(r -> r.headOption().map(var -> var.getValue())).getOrElse(Value.EMPTY);
	}
	
	/**
	 * Finds all indices that fulfill the specified condition
	 * @param table Searched table
	 * @param where Search condition
	 * @param connection DB Connection
	 * @return Result index values
	 * @throws DatabaseException
	 * @throws DatabaseUnavailableException
	 */
	public static ImmutableList<Value> indicesWhere(Table table, Condition where, Database connection) 
			throws DatabaseException, DatabaseUnavailableException
	{
		return select(Selection.indexFrom(table), table, where, connection)
				.flatMap(r -> r.headOption().map(var -> var.getValue()));
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
		ImmutableList<ImmutableList<ColumnVariable>> result = select(Selection.ALL, model.getTable(), 
				ImmutableList.empty(), Option.some(where), Option.some(1), Option.none(), connection);
		if (result.isEmpty())
			return false;
		else
		{
			model.addAttributes(result.head(), true);
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
		Condition where = new ComparisonCondition(model.getTable().getPrimaryColumn(), index);
		ImmutableList<ImmutableList<ColumnVariable>> result = select(Selection.ALL, model.getTable(), 
				ImmutableList.empty(), Option.some(where), Option.some(1), Option.none(), connection);
		if (result.isEmpty())
			return false;
		else
		{
			model.addAttributes(result.head(), true);
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
	public static int insert(ValueAssignment insert, Table into, Database connection) throws 
			DatabaseUnavailableException, DatabaseException
	{
		// If there are no values to insert or no table, does nothing
		if (into == null || insert == null)
			return -1;
			
		// Only table values are inserted. Auto-increment keys are removed
		ValueAssignment actualInsert = insert.filterToTable(into, true);
		if (actualInsert.isEmpty())
			return -1;
		
		// Makes sure all necessary columns are included (in update mode only primary key is required)
		if (!actualInsert.containsRequiredColumns(into))
			throw new DatabaseException(into, actualInsert);
		
		// Parses the sql
		String sql = actualInsert.toInsertClause(into);
		
		Database db = null;
		PreparedStatement statement = null;
		ResultSet results = null;
		try
		{
			db = openIfTemporary(into, connection);
			statement = db.getPreparedStatement(sql, into.usesAutoIncrementIndexing());
			
			// Inserts the values executes statement
			setStatementValues(statement, actualInsert);
			
			// Performs the query
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
		catch (SQLException | ValueInsertFailedException e)
		{
			throw new DatabaseException(e, sql.toString(), into, null, actualInsert, null);
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
			int index = insert(new ValueAssignment(true, model.getAttributes()), 
					model.getTable(), connection);
			
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
	 * @param where The condition which determines, which rows are deleted. None if all rows should be deleted.
	 * @param deleteFromJoined Should the joined rows be deleted as well
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed / was misused
	 */
	@SuppressWarnings("resource")
	public static void delete(Table from, ImmutableList<Join> joins, Option<Condition> where, 
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
		
		if (where != null && where.isDefined())
		{
			try
			{
				sql.append(where.get().toWhereClause());
			}
			catch (StatementParseException e)
			{
				throw new DatabaseException(e, where.get());
			}
		}
		
		Database db = null;
		PreparedStatement statement = null;
		try
		{
			db = openIfTemporary(from, connection);
			// Prepares the statement
			statement = db.getPreparedStatement(sql.toString());
			setStatementValues(statement, joins, where.toList());
			
			// Executes
			statement.executeUpdate();
		}
		catch (SQLException | ValueInsertFailedException e)
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
	 * @param where The condition which determines, which rows are deleted. None if all rows should be deleted
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws DatabaseException If the query failed / was misused
	 */
	public static void delete(Table from, Option<Condition> where, Database connection) 
			throws DatabaseUnavailableException, DatabaseException
	{
		delete(from, ImmutableList.empty(), where, false, connection);
	}
	
	/**
	 * Updates certain row(s) in the provided table
	 * @param table The table that is updated
	 * @param set The variable values that are set
	 * @param where The condition which determines which rows are updated. None if all rows should be updated.
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(Table table, ValueAssignment set, Option<Condition> where, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		update(table, ImmutableList.empty(), set, where, connection);
	}
	
	/**
	 * Updates certain row(s) in the provided table
	 * @param table The table that is updated
	 * @param joins The joins used in this query
	 * @param set The variable values that are set (non-table variables and  
	 * indices are automatically filtered)
	 * @param where The condition which determines which rows are updated. None if all rows should be updated
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the operation failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	@SuppressWarnings("resource")
	public static void update(Table table, ImmutableList<Join> joins, ValueAssignment set, 
			Option<Condition> where, Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		// Only updates attributes that belong to the target table(s) and are not primary 
		// keys / auto-increment keys
		ValueAssignment actualSet = set.filterToTables(table, joins, true);
		
		if (actualSet.isEmpty())
			return;
		
		// Parses the sql
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(table.getName());
		
		if (joins != null)
			appendJoin(sql, joins);
		sql.append(actualSet.toSetClause());
		
		if (where != null && where.isDefined())
		{
			try
			{
				sql.append(where.get().toWhereClause());
			}
			catch (StatementParseException e)
			{
				throw new DatabaseException(e, where.get());
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
			setStatementValues(statement, ImmutableList.flatten(joins, ImmutableList.withValue(actualSet), where.toList()));
			
			// Executes the update
			statement.executeUpdate();
		}
		catch (SQLException | ValueInsertFailedException e)
		{
			throw new DatabaseException(e, sql.toString(), table, where, actualSet, null);
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
	 * @param joins The joins used in this query
	 * @param where The condition with which the updated rows are selected. None if all rows should be updated
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, ImmutableList<Join> joins, Option<Condition> where, 
			boolean skipNullUpdates, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		update(model.getTable(), joins, new ValueAssignment(skipNullUpdates, model.getAttributes()), 
				where, connection);
	}
	
	/**
	 * Updates a models data into the database
	 * @param model The model who's attributes are written into the database
	 * @param where The condition with which the updated rows are selected. None if all rows should be updated
	 * @param skipNullUpdates Should null value attributes be skipped
	 * @param connection A database connection that should be used in the query. Null if a 
	 * temporary connection should be used. Only temporary connections are closed in this method.
	 * @throws DatabaseException If the process failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(TableModel model, Option<Condition> where, boolean skipNullUpdates, 
			Database connection) throws DatabaseException, DatabaseUnavailableException
	{
		update(model, ImmutableList.empty(), where, skipNullUpdates, connection);
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
		update(model, Option.some(new ComparisonCondition(model.getTable().getPrimaryColumn(), index)), 
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
		update(model, Option.some(ComparisonCondition.createIndexEqualsCondition(model)), skipNullUpdates, connection);
	}
	
	/**
	 * Checks whether a row can be found from the database
	 * @param table The table that is searched
	 * @param where The condition that is used for finding the row(s). None if there should be no filter.
	 * @param connection The database connection used (optional, temporary connection will be used 
	 * if null)
	 * @return Are there any rows that match the provided condition
	 * @throws DatabaseException If the query failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static boolean rowExists(Table table, Option<Condition> where, Database connection) throws 
			DatabaseException, DatabaseUnavailableException
	{
		ImmutableList<ImmutableList<ColumnVariable>> results = select(Selection.NONE, table, ImmutableList.empty(), 
				where, Option.some(1), Option.none(), connection);
		
		return !results.isEmpty();
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
		return rowExists(table, Option.some(ComparisonCondition.createIndexEqualsCondition(table, index)), 
				connection);
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
	
	private static void appendSelect(StringBuilder sql, Selection selection)
	{
		sql.append("SELECT ");
		if (selection == null || selection.selectsAll())
			sql.append("*");
		else if (selection.isEmpty())
			sql.append("NULL");
		else
			appendColumnNames(sql, selection.getColumns());
	}
	
	private static void appendColumnNames(StringBuilder sql, ImmutableList<? extends Column> columns)
	{
		if (!columns.isEmpty())
		{
			sql.append(columns.head().getColumnNameWithTable());
			columns.tail().forEach(c -> sql.append(", " + c.getColumnNameWithTable()));
		}
	}
	
	private static void appendJoin(StringBuilder sql, Iterable<Join> joins) throws DatabaseException
	{
		for (Join join : joins)
		{
			try
			{
				sql.append(join.toSql());
			}
			catch (StatementParseException e)
			{
				throw new DatabaseException(e, join.getJoinCondition());
			}
		}
	}
	
	private static void setStatementValues(PreparedStatement statement, ImmutableList<Join> joins, 
			ImmutableList<? extends PreparedSQLClause> otherClauses) throws ValueInsertFailedException
	{
		if (joins == null || joins.isEmpty())
			setStatementValues(statement, otherClauses);
		else
			setStatementValues(statement, ImmutableList.flatten(joins, otherClauses));
	}
	
	private static void setStatementValues(PreparedStatement statement, PreparedSQLClause... clauses) 
			throws ValueInsertFailedException
	{
		setStatementValues(statement, ImmutableList.of(clauses));
	}
	
	/**
	 * Inserts object values into a prepared statement
	 * @param statement The prepared statement
	 * @param clauses All the clauses used in the statement
	 * @throws SQLException If an sql error occurred
	 * @throws DatabaseException If some of the clause value couldn't be cast to compatible 
	 * data types
	 * @throws ValueInsertFailedException If insertion of a value failed
	 */
	private static void setStatementValues(PreparedStatement statement, ImmutableList<? extends PreparedSQLClause> clauses) 
			throws ValueInsertFailedException
	{
		// some clauses may be null (for example, no specified where condition, in which 
		// case they are skipped
		ImmutableList<? extends PreparedSQLClause> nonNullClauses = clauses.filter(c -> c != null);
		
		// May skip the whole process if no clauses have been specified
		if (nonNullClauses.isEmpty())
			return;
		
		// Collects required information
		SubTypeSet sqlTypes = SqlDataType.getSqlTypes();
		
		// Performs the value insert
		int index = 1;
		for (PreparedSQLClause clause : nonNullClauses)
		{
			for (Value value : clause.getValues())
			{
				// Casts each inserted value to a compatible data type
				try
				{
					Value castValue = value.castTo(sqlTypes);
					SqlDataType type = SqlDataType.castToSqlDataType(castValue.getType()).getOrFail(
							() -> new ValueInsertFailedException("No SQL counterpart for type: " + castValue.getType()));
					
					statement.setObject(index, castValue.getObjectValue(), type.getSqlType());
					index ++;
				}
				catch (DataTypeException e)
				{
					// Creates an exception if the value casting failed
					StringBuilder s = new StringBuilder();
					s.append("Value ");
					s.append(value.getDescription());
					s.append(" of clause ");
					try
					{
						s.append(clause.toSql());
					}
					catch (StatementParseException e1)
					{
						s.append("-- PARSING FAILED --");
					}
					s.append(" can't be cast to sql data type");
					
					throw new ValueInsertFailedException(s.toString(), e);
				}
				catch (SQLException e)
				{
					StringBuilder s = new StringBuilder();
					s.append("Failed to assign value ");
					s.append(value.getDescription());
					s.append("to clause ");
					s.append(clause.toString());
					s.append(" at index ");
					s.append(index);
					
					int valueAmount = 0;
					for (PreparedSQLClause sqlClause : clauses)
					{
						valueAmount += sqlClause.getValues().size();
					}
					
					s.append(". There are ");
					s.append(valueAmount);
					s.append(" values to set in total.");
					
					throw new ValueInsertFailedException(s.toString(), e);
				}
			}
		}
	}
	
	// NESTED CLASSES	---------------

	private static class ValueInsertFailedException extends Exception
	{
		private static final long serialVersionUID = -5237257223474389560L;

		public ValueInsertFailedException(String message)
		{
			super(message);
		}
		
		public ValueInsertFailedException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}
}