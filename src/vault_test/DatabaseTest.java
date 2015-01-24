package vault_test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import vault_database.DatabaseAccessor;
import vault_database.DatabaseSettings;
import vault_database.DatabaseTable;
import vault_database.DatabaseUnavailableException;

/**
 * This class tests the basic database features
 * 
 * @author Mikko Hilpinen
 * @since 23.1.2015
 */
public class DatabaseTest
{
	// CONSTRUCTOR	-----------------------------
	
	private DatabaseTest()
	{
		// Static interface
	}

	
	// MAIN METHOD	-----------------------------
	
	/**
	 * Starts the test
	 * @param args The first argument is the password, the second is the userName 
	 * (default: root) and the last is the server address 
	 * (default: jdbc:mysql://localhost:3306/)
	 */
	public static void main(String[] args)
	{
		String password = "";
		String user = "root";
		String address = "jdbc:mysql://localhost:3306/";
		
		if (args.length > 0)
			password = args[0];
		if (args.length > 1)
			user = args[1];
		if (args.length > 2)
			address = args[2];
		
		String[] possibleNames = {"one", "two", "three", "four", "five"};
		Integer[] possibleAdditionals = {1, 2, 3, 4, 5};
		
		try
		{
			DatabaseSettings.initialize(address, user, password, TestTable.values(), 10);
			
			// Inserts data
			insert(30, possibleNames, possibleAdditionals);
			// Updates data
			update();
			// Reads data
			read();
			// Finds data
			System.out.println("The number of 'ones': " + 
					DatabaseAccessor.findMatchingData(TestTable.DEFAULT, "name", "'one'", 
					"id").size());
			// Removes data
			removeTestData();
			
			System.out.println("OK!");
		}
		catch (DatabaseUnavailableException | SQLException e)
		{
			System.err.println("FAILURE!");
			e.printStackTrace();
		}
	}
	
	
	// OTHER METHODS	-------------------------
	
	private static void insert(int amount, String[] possibleNames, 
			Integer[] possibleAdditionals) throws SQLException, DatabaseUnavailableException
	{
		Random random = new Random();
		
		for (int i = 0; i < amount; i++)
		{
			DatabaseAccessor.insert(TestTable.DEFAULT, "'" + 
					possibleNames[random.nextInt(possibleNames.length)] + "', " + 
					possibleAdditionals[random.nextInt(possibleAdditionals.length)]);
		}
	}
	
	private static void read() throws DatabaseUnavailableException, SQLException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(TestTable.DEFAULT.getDatabaseName());
		PreparedStatement statement = null;
		ResultSet result = null;
		
		try
		{
			List<String> columnNames = DatabaseTable.readColumnNamesFromDatabase(TestTable.DEFAULT);
			
			for (int i = 1; i < DatabaseSettings.getTableHandler().getTableAmount(TestTable.DEFAULT); i++)
			{
				System.out.println("Table number " + i + ":");
				
				statement = accessor.getPreparedStatement("SELECT * FROM " + 
						TestTable.DEFAULT.getTableName() + i);
				result = statement.executeQuery();
				
				while (result.next())
				{
					StringBuilder row = new StringBuilder();
					for (String columnName : columnNames)
					{
						row.append(columnName + " = " + result.getString(columnName) + ", ");
					}
					System.out.println(row.toString());
				}
			}
		}
		finally
		{
			DatabaseAccessor.closeResults(result);
			DatabaseAccessor.closeStatement(statement);
			accessor.closeConnection();
		}
	}
	
	private static void removeTestData() throws SQLException, DatabaseUnavailableException
	{
		DatabaseAccessor accessor = new DatabaseAccessor(TestTable.DEFAULT.getDatabaseName());
		
		try
		{
			for (int i = 1; i < 
					DatabaseSettings.getTableHandler().getTableAmount(TestTable.DEFAULT); i++)
			{
				accessor.executeStatement("DELETE FROM " + 
						TestTable.DEFAULT.getTableName() + i);
			}
		}
		finally
		{
			accessor.closeConnection();
		}
	}
	
	private static void update() throws SQLException, DatabaseUnavailableException
	{
		DatabaseAccessor.update(TestTable.DEFAULT, "name", "'one'", "additional", "1");
	}
	
	
	// ENUMERATIONS	-----------------------------
	
	private static enum TestTable implements DatabaseTable
	{
		/**
		 * id (Auto-increment) | name | additional
		 */
		DEFAULT;

		@Override
		public boolean usesIndexing()
		{
			return true;
		}

		@Override
		public boolean usesAutoIncrementIndexing()
		{
			return true;
		}

		@Override
		public String getDatabaseName()
		{
			return "test_db";
		}

		@Override
		public String getTableName()
		{
			return "test";
		}
	}
}
