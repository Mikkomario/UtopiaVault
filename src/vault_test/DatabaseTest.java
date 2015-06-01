package vault_test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import vault_database.DatabaseAccessor;
import vault_database.DatabaseSettings;
import vault_database.DatabaseTable;
import vault_database.DatabaseUnavailableException;
import vault_database.InvalidTableTypeException;
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
			
			DatabaseSettings.initialize(address, user, password, 20);
			
			// Inserts data
			System.out.println("Inserts data");
			List<TestDatabaseEntity> data = insert(30, possibleNames, possibleAdditionals);
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
	
	private static List<TestDatabaseEntity> insert(int amount, String[] possibleNames, 
			Integer[] possibleAdditionals) throws SQLException, DatabaseUnavailableException
	{
		Random random = new Random();
		List<TestDatabaseEntity> data = new ArrayList<>();
		
		for (int i = 0; i < amount; i++)
		{
			data.add(new TestDatabaseEntity(possibleNames[random.nextInt(possibleNames.length)], 
					possibleAdditionals[random.nextInt(possibleAdditionals.length)]));
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
	
	
	// SUBCLASSES	-----------------------------
	
	private static class TestDatabaseEntity implements DatabaseReadable, DatabaseWritable
	{
		// ATTRIBUTES	------------------------
		
		private String id, name;
		private int additional;
		
		
		// CONSTRUCTOR	------------------------
		
		public TestDatabaseEntity(String name, int additional) throws SQLException, DatabaseUnavailableException
		{
			this.id = null;
			this.name = name;
			this.additional = additional;
			
			DatabaseAccessor.insert(this);
		}
		
		public TestDatabaseEntity(String id) throws DatabaseUnavailableException, SQLException
		{
			if (!DatabaseAccessor.readObjectData(this, id))
			{
				System.out.println(this);
				System.err.println("Couldn't find object data for id: " + id);
				this.name = "error";
				this.additional = 0;
			}
			
			this.id = id;
		}
		
		
		// IMPLEMENTED METHODS	--------------------
		
		@Override
		public String toString()
		{
			return this.name + "(" + this.id + ") " + this.additional;
		}
		
		@Override
		public String getColumnValue(String columnName)
		{
			switch (columnName)
			{
				case "id": return this.id;
				case "name": return this.name;
				case "additional": return this.additional + "";
			}
			
			System.err.println("Can't provide data for column " + columnName);
			return null;
		}

		@Override
		public void newIndexGenerated(int newIndex)
		{
			this.id = newIndex + "";
		}

		@Override
		public DatabaseTable getTable()
		{
			return TestTable.DEFAULT;
		}

		@Override
		public void setValue(String columnName, String readValue)
		{
			switch (columnName)
			{
				case "id": this.id = readValue; break;
				case "name": this.name = readValue; break;
				// TODO: "five" is given here for some random reason
				case "additional": this.additional = Integer.parseInt(readValue); break;
			}
		}
		
		
		// OTHER METHODS	---------------------
		
		public void delete() throws SQLException, DatabaseUnavailableException
		{
			DatabaseAccessor.delete(getTable(), getTable().getPrimaryColumnName(), this.id);
		}
		
		public void changeAdditional(int newAdditional) throws InvalidTableTypeException, 
				SQLException, DatabaseUnavailableException
		{
			this.additional = newAdditional;
			DatabaseAccessor.update(this);
		}
	}
	
	
	// ENUMERATIONS	-----------------------------
	
	private static enum TestTable implements DatabaseTable
	{
		/**
		 * id (Auto-increment) | name | additional
		 */
		DEFAULT;

		@Override
		public boolean usesIntegerIndexing()
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

		@Override
		public List<String> getColumnNames()
		{
			try
			{
				return DatabaseTable.getColumnNamesFromColumnInfo(
						DatabaseTable.readColumnInfoFromDatabase(this));
			}
			catch (DatabaseUnavailableException | SQLException e)
			{
				System.err.println("Can't read column names");
				e.printStackTrace();
				return new ArrayList<>();
			}
		}

		@Override
		public String getPrimaryColumnName()
		{
			return "id";
		}
	}
}
