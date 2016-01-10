package vault_test;

import vault_database_old.DatabaseSettings;
import vault_database_old.AttributeNameMapping.NoAttributeForColumnException;
import vault_recording.DatabaseModel;

/**
 * This class tests the attribute initialization in databaseModel
 * @author Mikko Hilpinen
 * @since 29.9.2015
 */
public class ModelAttributeTest
{
	private ModelAttributeTest()
	{
		// Static interface
	}

	/**
	 * Starts the test
	 * @param args Not used
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
		
		try
		{
			DatabaseSettings.initialize(address, user, password, null);
			
			DatabaseModel testModel = new DatabaseModel(TestTable.DEFAULT);
			testModel.initializeTableAttributesToNull(true);
			
			System.out.println("Attributes read: " + testModel.getAttributes().size());
			
			System.out.println("OK");
		}
		catch (NoAttributeForColumnException e)
		{
			System.err.println("Failure!");
			e.printStackTrace();
		}
	}
}
