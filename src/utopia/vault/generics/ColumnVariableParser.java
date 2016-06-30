package utopia.vault.generics;

import utopia.flow.generics.Value;
import utopia.flow.generics.VariableParser;
import utopia.vault.generics.Table.NoSuchColumnException;

/**
 * This parser is able to generate column variables
 * @author Mikko Hilpinen
 * @since 28.6.2016
 */
public class ColumnVariableParser implements VariableParser<ColumnVariable>
{
	// ATTRIBUTES	------------------
	
	private Table table;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new variable parser
	 * @param table The table the parser fetches columns from
	 */
	public ColumnVariableParser(Table table)
	{
		this.table = table;
	}
	
	
	// IMPLEMENTED METHODS	----------

	@Override
	public ColumnVariable generate(String variableName)
			throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(getColumn(variableName));
	}

	@Override
	public ColumnVariable generate(String variableName, Value value)
			throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(getColumn(variableName), value);
	}

	@Override
	public ColumnVariable copy(ColumnVariable variable)
			throws utopia.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(variable);
	}
	
	
	// OTHER METHODS	---------------
	
	private Column getColumn(String variableName)
	{
		try
		{
			return this.table.findColumnWithVariableName(variableName);
		}
		catch (NoSuchColumnException e)
		{
			throw new VariableGenerationFailedException("Can't find column for variable '" + 
					variableName + "' from table " + this.table, e);
		}
	}
}
