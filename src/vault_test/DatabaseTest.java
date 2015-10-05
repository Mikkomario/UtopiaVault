package vault_test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import vault_database.Attribute;
import vault_database.AttributeNameEqualsWhereCondition;
import vault_database.AttributeNameMapping.MappingException;
import vault_database.AttributeNameMapping.NoAttributeForColumnException;
import vault_database.DataType;
import vault_database.DatabaseAccessor;
import vault_database.DatabaseException;
import vault_database.DatabaseSettings;
import vault_database.DatabaseValue;
import vault_database.EqualsWhereCondition.Operator;
import vault_database.IndexAttributeRequiredException;
import vault_database.Attribute.AttributeDescription;
import vault_database.DatabaseUnavailableException;
import vault_database.WhereCondition;
import vault_database.WhereCondition.WhereConditionParseException;
import vault_recording.DatabaseModel;

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
		Integer[] possibleAdditionals = {null, 2, 3, 4, 5};
		
		try
		{
			System.out.println("Initializes settings");
			
			DatabaseSettings.initialize(address, user, password);
			
			System.out.println("Integer type: " + Types.INTEGER);
			System.out.println("Varchar type: " + Types.VARCHAR);
			System.out.println("Other type: " + Types.OTHER);
			
			// Inserts data
			System.out.println("Inserts data");
			List<DatabaseModel> data = insert(10, possibleNames, possibleAdditionals);
			print(data);
			// Reads data
			System.out.println("Reads data");
			List<DatabaseModel> readData = read();
			print(readData);
			// Updates data
			System.out.println("Updates data");
			update(data);
			// Reads data
			System.out.println("Reads data");
			readData.clear();
			readData = read();
			print(readData);
			// Finds data
			System.out.println("Finds data");
			System.out.println("The number of 'ones': " + 
					numberOfModelsWithName("one"));
			// Removes data
			System.out.println("Removes (read) data");
			removeTestData(readData);
			// Reads data
			System.out.println("Reads data");
			readData.clear();
			readData = read();
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
		catch (MappingException e)
		{
			System.err.println("Failure. Attribute name mapping failed");
			e.printStackTrace();
		}
		catch (WhereConditionParseException e)
		{
			System.err.println("Failure. Where condition couldn't be parsed");
			e.printStackTrace();
		}
	}
	
	
	// OTHER METHODS	-------------------------
	
	private static List<DatabaseModel> insert(int amount, String[] possibleNames, 
			Integer[] possibleAdditionals) throws 
			DatabaseUnavailableException, IndexAttributeRequiredException, DatabaseException
	{
		Random random = new Random();
		List<DatabaseModel> data = new ArrayList<>();
		//List<ColumnInfo> columnInfo = TestTable.DEFAULT.getColumnInfo();
		
		for (int i = 0; i < amount; i++)
		{
			DatabaseModel model = new DatabaseModel(TestTable.DEFAULT, true);
			// Adds name & additional
			/*
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
			*/
			//model.setAttributeValue("id", null, true);
			model.setAttributeValue("name", 
					possibleNames[random.nextInt(possibleNames.length)], true);
			model.setAttributeValue("additional", 
					possibleAdditionals[random.nextInt(possibleAdditionals.length)], true);
			
			// Writes the model into database
			DatabaseAccessor.updateObjectToDatabase(model, false);
			
			data.add(model);
		}
		
		return data;
	}
	
	private static List<DatabaseModel> read() throws 
			DatabaseUnavailableException, DatabaseException, NoAttributeForColumnException, 
			WhereConditionParseException
	{
		// Finds out all the id's
		AttributeDescription indexDescription = new AttributeDescription(
				TestTable.DEFAULT.getPrimaryColumn(), 
				TestTable.DEFAULT.getAttributeNameMapping());
		List<List<Attribute>> ids = DatabaseAccessor.select(indexDescription.wrapIntoList(), 
				TestTable.DEFAULT, -1);
		
		System.out.println("Found " + ids.size() + " ids");
		
		// Loads the models
		List<DatabaseModel> models = new ArrayList<>();
		for (List<Attribute> idResult : ids)
		{
			DatabaseModel model = new DatabaseModel(TestTable.DEFAULT, true);
			DatabaseAccessor.readObjectAttributesFromDatabase(model, idResult.get(0).getValue());
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
	
	private static int numberOfModelsWithName(String name) throws DatabaseUnavailableException, 
			DatabaseException, NoAttributeForColumnException, WhereConditionParseException
	{
		//List<Attribute> whereAttributes = new Attribute(Attribute.getTableAttributeDescription(
		//		TestTable.DEFAULT, "name"), name).wrapIntoList();
		WhereCondition where = new AttributeNameEqualsWhereCondition(Operator.EQUALS, 
				"name", new DatabaseValue(DataType.STRING, name));
				//new Attribute(mapping.findColumnForAttribute(
				//TestTable.DEFAULT.getColumnInfo(), "name"), mapping, name).wrapIntoList();
		List<AttributeDescription> selection = new AttributeDescription(
				TestTable.DEFAULT.getPrimaryColumn(), 
				TestTable.DEFAULT.getAttributeNameMapping()).wrapIntoList();
		return DatabaseAccessor.select(selection, TestTable.DEFAULT, where, -1).size();
	}
}
