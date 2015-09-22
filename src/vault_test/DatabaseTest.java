package vault_test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import vault_database.DatabaseAccessor;
import vault_database.DatabaseException;
import vault_database.DatabaseSettings;
import vault_database.DatabaseTable;
import vault_database.DatabaseTable.ColumnInfo;
import vault_database.DatabaseUnavailableException;
import vault_recording.Attribute;
import vault_recording.Attribute.AttributeDescription;
import vault_recording.AttributeNameMapping;
import vault_recording.DatabaseModel;
import vault_recording.IndexAttributeRequiredException;

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
			AttributeNameMapping mappings = new AttributeNameMapping();
			for (ColumnInfo column : TestTable.DEFAULT.getColumnInfo())
			{
				System.out.println("Mapping " + column.getName());
				System.out.println("Type = " + column.getType());
				mappings.addMapping(column.getName(), column.getName().substring(5));
			}
			System.out.println("Integer type: " + Types.INTEGER);
			System.out.println("Varchar type: " + Types.VARCHAR);
			System.out.println("Other type: " + Types.OTHER);
			
			// Inserts data
			System.out.println("Inserts data");
			List<DatabaseModel> data = insert(10, possibleNames, possibleAdditionals, mappings);
			print(data);
			// Reads data
			System.out.println("Reads data");
			List<DatabaseModel> readData = read(mappings);
			print(readData);
			// Updates data
			System.out.println("Updates data");
			update(data);
			// Reads data
			System.out.println("Reads data");
			readData.clear();
			readData = read(mappings);
			print(readData);
			// Finds data
			System.out.println("Finds data");
			System.out.println("The number of 'ones': " + 
					numberOfModelsWithName("one", mappings));
			// Removes data
			System.out.println("Removes (read) data");
			removeTestData(readData);
			// Reads data
			System.out.println("Reads data");
			readData.clear();
			readData = read(mappings);
			print(readData);
			
			System.out.println("OK!");
		}
		catch (DatabaseException e)
		{
			System.err.println("Failure due to databaseException");
			System.err.println(e.getDebugMessage());
			e.printStackTrace();
		}
		catch (DatabaseUnavailableException e)
		{
			System.err.println("Failure. Can't connect to DB");
			e.printStackTrace();
		}
		catch (IndexAttributeRequiredException e)
		{
			System.err.println("Failure. No index.");
			if (e.getSourceObject() != null)
			{
				for (Attribute attribute : e.getSourceObject().getAttributes())
				{
					System.err.println(attribute);
				}
			}
			e.printStackTrace();
		}
	}
	
	
	// OTHER METHODS	-------------------------
	
	private static List<DatabaseModel> insert(int amount, String[] possibleNames, 
			Integer[] possibleAdditionals, AttributeNameMapping attributeMapping) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		Random random = new Random();
		List<DatabaseModel> data = new ArrayList<>();
		List<ColumnInfo> columnInfo = TestTable.DEFAULT.getColumnInfo();
		
		for (int i = 0; i < amount; i++)
		{
			DatabaseModel model = new DatabaseModel(TestTable.DEFAULT, true, attributeMapping);
			// Adds name & additional
			for (ColumnInfo column : columnInfo)
			{
				String attributeName = attributeMapping.getAttributeName(column.getName());
				Object value = null;
				switch (attributeName)
				{
					case "name": value = 
							possibleNames[random.nextInt(possibleNames.length)]; break;
					case "additional": value = 
							possibleAdditionals[random.nextInt(possibleAdditionals.length)]; break;
					default: continue;
				}
				
				model.addAttribute(new Attribute(column, attributeName, value), true);
			}
			
			// Writes the model into database
			DatabaseAccessor.updateObjectToDatabase(model, false);
			
			data.add(model);
		}
		
		return data;
	}
	
	private static List<DatabaseModel> read(AttributeNameMapping mapping) throws 
			DatabaseUnavailableException, DatabaseException
	{
		// Finds out all the id's
		AttributeDescription indexDescription = new AttributeDescription(
				TestTable.DEFAULT.getPrimaryColumn(), mapping);
		List<List<Attribute>> ids = DatabaseAccessor.select(indexDescription.wrapIntoList(), 
				TestTable.DEFAULT, -1);
		
		System.out.println("Found " + ids.size() + " ids");
		
		// Loads the models
		List<DatabaseModel> models = new ArrayList<>();
		for (List<Attribute> id : ids)
		{
			DatabaseModel model = new DatabaseModel(TestTable.DEFAULT, true, mapping);
			DatabaseAccessor.readObjectAttributesFromDatabase(model, id);
			models.add(model);
		}
		
		return models;
	}
	
	private static void removeTestData(List<DatabaseModel> data) throws  
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		for (DatabaseModel model : data)
		{
			DatabaseAccessor.deleteObjectFromDatabase(model);
		}
	}
	
	private static void update(List<DatabaseModel> data) throws DatabaseUnavailableException, 
			IndexAttributeRequiredException, DatabaseException
	{
		for (DatabaseModel model: data)
		{
			model.getAttribute("additional").setValue(7);
			DatabaseAccessor.updateObjectToDatabase(model, true);
		}
	}
	
	private static void print(List<DatabaseModel> data)
	{
		for (DatabaseModel model : data)
		{
			System.out.println("Model: ");
			for (Attribute attribute : model.getAttributes())
			{
				System.out.println(attribute);
			}
		}
	}
	
	private static int numberOfModelsWithName(String name, 
			AttributeNameMapping mapping) throws DatabaseUnavailableException, DatabaseException
	{
		List<Attribute> whereAttributes = new Attribute(mapping.findColumnForAttribute(
				TestTable.DEFAULT.getColumnInfo(), "name"), mapping, name).wrapIntoList();
		List<AttributeDescription> selection = new AttributeDescription(
				TestTable.DEFAULT.getPrimaryColumn()).wrapIntoList();
		return DatabaseAccessor.select(selection, TestTable.DEFAULT, whereAttributes, -1).size();
	}
	
	
	// ENUMERATIONS	-----------------------------
	
	private static enum TestTable implements DatabaseTable
	{
		/**
		 * test_id (Auto-increment) | test_name | test_additional
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
		public ColumnInfo getPrimaryColumn()
		{
			return DatabaseTable.findPrimaryColumnInfo(getColumnInfo());
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
