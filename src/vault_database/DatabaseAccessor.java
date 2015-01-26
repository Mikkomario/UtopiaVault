package vault_database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import vault_database.DatabaseSettings.UninitializedSettingsException;

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
	private boolean open;
	
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
		this.open = false;
	}

	
	// OTHER METHODS	--------------------------------------------------
	
	/**
	 * This method opens a connection to the formerly specified database. The 
	 * connection must be closed with {@link #closeConnection()} after the necessary 
	 * statements have been executed.
	 * @throws DatabaseUnavailableException If the connection couldn't be opened
	 * @see #closeConnection()
	 */
	private void openConnection() throws DatabaseUnavailableException
	{
		// If a connection is already open, quits
		if (this.open)
			return;
		
		// Tries to form a connection to the database
		try
		{
			this.currentConnection = DriverManager.getConnection(
					DatabaseSettings.getConnectionTarget() + this.databaseName, 
					DatabaseSettings.getUser(), DatabaseSettings.getPassword());
		}
		catch (SQLException | UninitializedSettingsException e)
		{
			throw new DatabaseUnavailableException(e);
		}
		
		this.open = true;
	}
	
	/**
	 * Closes a currently open connection to the database.
	 */
	public void closeConnection()
	{
		// If there is no connection, quits
		if (!this.open)
			return;
		
		// Closes the connection
		try
		{
			if (this.currentConnection != null)
				this.currentConnection.close();
		}
		catch (SQLException e)
		{
			System.err.println("DatabaseAccessProvider failed to close a "
					+ "connection to " + this.databaseName);
			e.printStackTrace();
			return;
		}
		
		this.open = false;
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
		// Opens the connection if it isn't already
		if (!this.open)
			openConnection();
		
		if (sqlStatement == null)
			return;
		
		Statement statement = null;
		
		try
		{
			statement = this.currentConnection.createStatement();
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
			boolean returnAutogeneratedKeys) throws DatabaseUnavailableException, SQLException
	{
		// Opens the connection if it isn't already
		if (!this.open)
			openConnection();
		
		if (sqlStatement == null)
			throw new SQLException("No statement provided");
		
		PreparedStatement statement = null;
		
		try
		{
			int autoGeneratedKeys = Statement.NO_GENERATED_KEYS;
			if (returnAutogeneratedKeys)
				autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS;
			
			statement = this.currentConnection.prepareStatement(sqlStatement, 
					autoGeneratedKeys);
			
			return statement;
		}
		catch (SQLException e)
		{
			// Closes the statement before throwing the exception
			closeStatement(statement);
			throw e;
		}
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
		// The change is simple when a connection is closed
		if (!this.open)
			this.databaseName = newDatabaseName;
		// When a connection is open, informs the server
		else
		{
			try
			{
				executeStatement("USE " + newDatabaseName + ";");
				this.databaseName = newDatabaseName;
			}
			catch (SQLException | DatabaseUnavailableException e)
			{
				System.err.println("DatabaseAccessor failed to change the database name");
				e.printStackTrace();
			}
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
	 * Performs a search through all the tables of a certain type in order to find the 
	 * matching column data.
	 * 
	 * @param table The table type that should contain the information
	 * @param keyColumn The name of the column where the key is
	 * @param keyValue The value which the column must have in order for the row to make it 
	 * to the result
	 * @param valueColumn From which column the returned value is extracted from
	 * @return A list containing all the collected values
	 * @throws SQLException If the given values were invalid
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static List<String> findMatchingData(DatabaseTable table, String keyColumn, 
			String keyValue, String valueColumn) throws DatabaseUnavailableException, 
			SQLException
	{
		String[] keyColumns = {keyColumn};
		String[] keyValues = {keyValue};
		
		return findMatchingData(table, keyColumns, keyValues, valueColumn);
	}
	
	/**
	 * Performs a search through all the tables of a certain type in order to find the 
	 * matching column data.
	 * 
	 * @param table The table type that should contain the information
	 * @param keyColumns The names of the columns where the keys are
	 * @param keyValues The values which the columns must have in order for the row to make 
	 * it to the result
	 * @param valueColumn From which column the returned value is extracted from
	 * @return A list containing all the collected values
	 * @throws SQLException If the given values were invalid
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static List<String> findMatchingData(DatabaseTable table, String[] keyColumns, 
			String[] keyValues, String valueColumn) throws DatabaseUnavailableException, 
			SQLException
	{
		List<String> resultData = new ArrayList<>();
		
		// Performs a database query
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		
		int tables = DatabaseSettings.getTableHandler().getTableAmount(table);
		
		// Goes through all the userData tables in order
		for (int i = 1; i <= tables; i++)
		{	
			StringBuilder statementString = new StringBuilder("SELECT * FROM " + 
					table.getTableName() + i + " WHERE ");
			for (int keyIndex = 0; keyIndex < keyColumns.length; keyIndex ++)
			{
				if (keyIndex != 0)
					statementString.append(" AND ");
				statementString.append(keyColumns[keyIndex] + " = " + keyValues[keyIndex]);
			}
			
			PreparedStatement statement = 
					accessor.getPreparedStatement(statementString.toString());
			ResultSet results = null;
			
			results = statement.executeQuery();
			
			while (results.next())
			{
				resultData.add(results.getString(valueColumn));
			}
			
			DatabaseAccessor.closeResults(results);
			DatabaseAccessor.closeStatement(statement);
		}
		
		accessor.closeConnection();
		return resultData;
	}
	
	/**
	 * Inserts new data to the given table. In case of an indexed table, remember to also call 
	 * {@link MultiTableHandler#informAboutNewRow(DatabaseTable, int)} once the method has 
	 * completed.
	 * @param targetTable The table the data will be inserted into
	 * @param columnData The data posted to the table (in the same order as in the tables). 
	 * All data will be considered to be strings
	 * @throws SQLException If the insert failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void insert(DatabaseTable targetTable, List<String> columnData) throws 
			SQLException, DatabaseUnavailableException
	{
		insert(targetTable, getColumnDataString(columnData));
	}
	
	/**
	 * Inserts new data to the given table. In case of an indexed table, remember to also call 
	 * {@link MultiTableHandler#informAboutNewRow(DatabaseTable, int)} once the method has 
	 * completed.
	 * @param targetTable The table the data will be inserted into
	 * @param columnData The data posted to the table (in the same order as in the tables). 
	 * Each value should be separated with a ','. Auto-increment id shouldn't be included. 
	 * If the value is to be considered a string, it should be surrounded by "'" brackets. 
	 * For example "'one', 2, 'three'".
	 * @throws SQLException If the insert failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void insert(DatabaseTable targetTable, String columnData) throws 
			SQLException, DatabaseUnavailableException
	{
		// Opens connection
		DatabaseAccessor accessor = new DatabaseAccessor(targetTable.getDatabaseName());
		
		// Inserts data
		try
		{
			accessor.executeStatement(getInsertStatement(targetTable, columnData));
		}
		finally
		{
			// Closes connection
			accessor.closeConnection();
		}
	}
	
	/**
	 * Inserts new data into the given table while also collecting and returning the 
	 * auto-generated id. Also informs the multiTableHandler about the addition. This should 
	 * be used for auto-indexing tables.
	 * @param targetTable The table the data is inserted into
	 * @param columnData The data inserted into the table
	 * @param idColumnName The name of the auto-increment id column
	 * @return The id generated during the insert
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws SQLException If the insert failed or if the table doesn't use auto-increment 
	 * indexing
	 */
	public static int insert(DatabaseTable targetTable, List<String> columnData, 
			String idColumnName) throws DatabaseUnavailableException, SQLException
	{
		return insert(targetTable, getColumnDataString(columnData), idColumnName);
	}
	
	/**
	 * Inserts new data into the given table while also collecting and returning the 
	 * auto-generated id. Also informs the multiTableHandler about the addition. This 
	 * should be used for auto-indexing tables.
	 * @param targetTable The table the data is inserted into
	 * @param columnData The data posted to the table (in the same order as in the tables). 
	 * Each value should be separated with a ','. Auto-increment id shouldn't be included. 
	 * If the value is to be considered a string, it should be surrounded by "'" brackets. 
	 * For example "'one', 2, 'three'".
	 * @param idColumnName The name of the auto-increment id column
	 * @return The id generated during the insert
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 * @throws SQLException If the insert failed or if the table doesn't use auto-increment 
	 * indexing
	 */
	public static int insert(DatabaseTable targetTable, String columnData, 
			String idColumnName) throws SQLException, DatabaseUnavailableException
	{
		// Doesn't work if the table doesn't use auto-increment
		if (!targetTable.usesAutoIncrementIndexing())
			throw new SQLException("The table " + targetTable.getTableName() + 
					" doesn't use auto-increment indexing so no id can be retrieved");
		
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
				int id = autoKeys.getInt(idColumnName);
				DatabaseSettings.getTableHandler().informAboutNewRow(targetTable, id);
				return id;
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
	
	/**
	 * Deletes certain data from all the tables of the given type
	 * @param targetTable The table type from which data is removed
	 * @param targetColumn The column which is used for checking whether data should be deleted.
	 * @param targetValue The value which the column must have in order for the data to be 
	 * deleted
	 * @throws SQLException If the deletion failed
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void delete(DatabaseTable targetTable, String targetColumn, 
			String targetValue) throws SQLException, DatabaseUnavailableException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(targetTable.getDatabaseName());
		
		try
		{
			for (int i = 1; i <= DatabaseSettings.getTableHandler().getTableAmount(targetTable); i++)
			{
				StringBuilder statement = new StringBuilder("DELETE from ");
				statement.append(targetTable.getTableName());
				statement.append(i);
				statement.append(" WHERE ");
				statement.append(targetColumn);
				statement.append(" = ");
				statement.append(targetValue);
				statement.append(";");
				
				accessor.executeStatement(statement.toString());
			}
		}
		finally
		{
			accessor.closeConnection();
		}
	}
	
	/**
	 * Updates a value in a column for certain rows
	 * @param table The table in which the update is done
	 * @param targetColumn The column which should contain the targetValue
	 * @param targetValue The value the column should have in order to be updated
	 * @param changeColumn The column which is being updated
	 * @param newValue The new value given to the changeColumn
	 * @throws SQLException If the update fails
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(DatabaseTable table, String targetColumn, 
			String targetValue, String changeColumn, String newValue) throws SQLException, 
			DatabaseUnavailableException
	{
		String[] changeColumns = {changeColumn};
		String[] newValues = {newValue};
		update(table, targetColumn, targetValue, changeColumns, newValues);
	}
	
	/**
	 * Updates a set of columns for certain rows
	 * @param table The table in which the update is done
	 * @param targetColumn The column which should contain the targetValue
	 * @param targetValue The value the column should have in order to be updated
	 * @param changeColumns The columns which are being updated
	 * @param newValues The new values given to the changeColumns
	 * @throws SQLException If the update fails
	 * @throws DatabaseUnavailableException If the database couldn't be accessed
	 */
	public static void update(DatabaseTable table, String targetColumn, String targetValue, 
			String[] changeColumns, String[] newValues) throws SQLException, DatabaseUnavailableException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(table.getDatabaseName());
		
		try
		{
			for (int i = 1; i <= DatabaseSettings.getTableHandler().getTableAmount(table); i++)
			{
				StringBuilder statementBuilder = new StringBuilder("UPDATE ");
				statementBuilder.append(table.getTableName() + i);
				statementBuilder.append(" SET ");
				
				for (int columnIndex = 0; columnIndex < changeColumns.length; columnIndex ++)
				{
					if (columnIndex != 0)
						statementBuilder.append(", ");
					statementBuilder.append(changeColumns[columnIndex] + " = " + 
						newValues[columnIndex]);
				}
				
				statementBuilder.append(" WHERE " + targetColumn + " = " + targetValue);
				
				accessor.executeStatement(statementBuilder.toString());
			}
		}
		finally
		{
			accessor.closeConnection();
		}
	}
	
	private static String getColumnDataString(List<String> columnData)
	{
		StringBuilder columnDataString = new StringBuilder();
		
		for (int i = 0; i < columnData.size(); i++)
		{
			if (i != 0)
				columnDataString.append(", ");
			columnDataString.append("'" + columnData + "'");
		}
		
		return columnDataString.toString();
	}
	
	/*
	private static String getInsertStatement(DatabaseTable table, List<String> columnData)
	{
		return getInsertStatement(table, getColumnDataString(columnData));
	}
	*/
	
	private static String getInsertStatement(DatabaseTable table, String columnDataString)
	{
		StringBuilder statement = new StringBuilder();
		statement.append("INSERT INTO ");
		statement.append(DatabaseSettings.getTableHandler().getLatestTableName(table));
		statement.append(" values (");
		
		if (table.usesAutoIncrementIndexing())
			statement.append("null, ");
		
		statement.append(columnDataString);
		
		statement.append(");");
		
		return statement.toString();
	}
}
