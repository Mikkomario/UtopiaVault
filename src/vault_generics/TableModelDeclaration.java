package vault_generics;

import java.util.Collection;

import utopia.flow.generics.ModelDeclaration;

/**
 * This is an immutable model declaration that is used primarily declaration of a set of 
 * database variables
 * @author Mikko Hilpinen
 * @since 9.1.2016
 */
public class TableModelDeclaration extends ModelDeclaration<Column>
{
	// CONSTRUCTOR	-------------------
	
	/**
	 * Creates a new declaration
	 * @param attributeDeclarations The columns that should be included in this declaration
	 */
	public TableModelDeclaration(Collection<? extends Column> attributeDeclarations)
	{
		super(attributeDeclarations);
	}
	
	/**
	 * Wraps an instance of the super class
	 * @param declaration A declaration that should be wrapped
	 */
	public TableModelDeclaration(ModelDeclaration<Column> declaration)
	{
		super(declaration.getAttributeDeclarations());
	}
}
