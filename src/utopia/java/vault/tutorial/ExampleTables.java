package utopia.java.vault.tutorial;

import utopia.java.flow.structure.Option;
import utopia.java.vault.database.DatabaseReferenceReader;
import utopia.java.vault.database.ReadFromDatabaseColumnInitialiser;
import utopia.java.vault.generics.Table;
import utopia.java.vault.generics.UnderlinesToCamelCaseRule;
import utopia.java.vault.generics.VariableNameMapping;

/**
 * This class is a static accessor for the tables used in the tutorials
 * @author Mikko Hilpinen
 * @since 21.1.2016
 */
public class ExampleTables
{
	// ATTRIBUTES	-------------
	
	/**
	 * The user table. Contains columns "users_id", "users_name" and "users_role". Last of 
	 * which references the ExampleTable.ROLES
	 */
	public static final Table USERS = createTable("users");
	/**
	 * The role table. Contains columns "role_id" and "role_name".
	 */
	public static final Table ROLES = createTable("roles");

	
	// OTHER METHODS	---------
	
	private static Table createTable(String name)
	{
		// Creates the name mapping first
		VariableNameMapping mapping = new VariableNameMapping();
		mapping.addRule(UnderlinesToCamelCaseRule.getInstance());
		
		return new Table("test_db", name, mapping, new ReadFromDatabaseColumnInitialiser(null),
				Option.some(new DatabaseReferenceReader()));
	}
}
