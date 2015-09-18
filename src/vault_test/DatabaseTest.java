package vault_test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import vault_database.DatabaseAccessor;
import vault_database.DatabaseSettings;
import vault_database.DatabaseTable;
import vault_database.DatabaseUnavailableException;
import vault_database.InvalidTableTypeException;
import vault_recording.DatabaseModel;
import vault_recording.DatabaseReadable;
import vault_recording.DatabaseWritable;

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
		
		if (args.length == 0)
		{
			System.out.println("Please provide the necessary arguments: password (required), "
					+ "user (default = root), address (default = jdbc:mysql://localhost:3306/)");
			System.exit(0);
		}
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
			System.out.println("Initializes settings");
			
			DatabaseSettings.initialize(address, user, password);
			
			System.out.println("Creates attribute mappings");
			Map<String, String> mappings = new HashMap<>();
			for (String columnName : TestTable.DEFAULT.getColumnNames())
			{
				mappings.put(columnName, columnName.substring(3));
			}
			
			// Inserts data
			System.out.println("Inserts data");
			List<DatabaseModel> data = insert(30, possibleNames, possibleAdditionals);
			// Reads data
			System.out.println("Reads data");
			read();
			// Updates data
			System.out.println("Updates data");
			update(data);
			// Reads data
			System.out.println("Reads data");
			read();
			// Finds data
			System.out.println("Finds data");
			System.out.println("The number of 'ones': " + 
					DatabaseAccessor.findMatchingData(TestTable.DEFAULT, "name", "one", 
					"id").size());
			// Removes data
			System.out.println("Removes data");
			removeTestData(data);
			// Reads data
			System.out.println("Reads data");
			read();
			
			System.out.println("OK!");
		}
		catch (Exception e)
		{
			System.err.println("FAILURE!");
			e.printStackTrace();
		}
	}
	
	
	// OTHER METHODS	-------------------------
	
	private static List<DatabaseModel> insert(int amount, String[] possibleNames, 
			Integer[] possibleAdditionals, Map<String, String> attributeMapping) throws 
			SQLException, DatabaseUnavailableException
	{
		Random random = new Random();
		List<DatabaseModel> data = new ArrayList<>();
		
		for (int i = 0; i < amount; i++)
		{
			// TODO: Continue
			//data.add(new TestDatabaseEntity(possibleNames[random.nextInt(possibleNames.length)], 
			//		possibleAdditionals[random.nextInt(possibleAdditionals.length)]));
		}
		
		return data;
	}
	
	private static void read() throws DatabaseUnavailableException, SQLException, InvalidTableTypeException
	{
		List<String> ids = DatabaseAccessor.findMatchingIDs(TestTable.DEFAULT, new String[0], 
				new String[0]);
		
		for (String id : ids)
		{
			System.out.println(new TestDatabaseEntity(id));
		}
	}
	
	private static void removeTestData(List<TestDatabaseEntity> data) throws SQLException, 
			DatabaseUnavailableException
	{
		for (TestDatabaseEntity entity : data)
		{
			entity.delete();
		}
	}
	
	private static void update(List<TestDatabaseEntity> data) throws 
			InvalidTableTypeException, SQLException, DatabaseUnavailableException
	{
		for (TestDatabaseEntity entity: data)
		{
			entity.changeAdditional(7);
		}
	}
	
	
	// ENUMERATIONS	-----------------------------
	
	private static enum TestTable implements DatabaseTable
	{
		/**
		 * db_id (Auto-increment) | db_name | db_additional
		 */
		DEFAULT;
		
		
		// ATTRIBUTES	------------------
		
		private static List<ColumnInfo> columnInfo;
		
		
		// IMPLEMENTED METHODS	----------

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

		@Override
		public List<String> getColumnNames()
		{
			return DatabaseTable.getColumnNamesFromColumnInfo(getColumnInfo());
		}

		@Override
		public String getPrimaryColumnName()
		{
			return DatabaseTable.findPrimaryColumnInfo(getColumnInfo()).getName();
		}

		@Override
		public List<ColumnInfo> getColumnInfo()
		{
			if (columnInfo == null)
			{
				try
				{
					columnInfo = DatabaseTable.readColumnInfoFromDatabase(this);
				}
				catch (DatabaseUnavailableException | SQLException e)
				{
					System.out.println("Couldn't read column info");
					e.printStackTrace();
				}
			}
			
			return columnInfo;
		}
	}
}
