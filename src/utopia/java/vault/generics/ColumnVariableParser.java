package utopia.java.vault.generics;

import utopia.java.flow.generics.Value;
import utopia.java.flow.generics.VariableParser;

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
			throws utopia.java.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(getColumn(variableName));
	}

	@Override
	public ColumnVariable generate(String variableName, Value value)
			throws utopia.java.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(getColumn(variableName), value);
	}

	@Override
	public ColumnVariable copy(ColumnVariable variable)
			throws utopia.java.flow.generics.VariableParser.VariableGenerationFailedException
	{
		return new ColumnVariable(variable);
	}
	
	
	// OTHER METHODS	---------------
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.table == null) ? 0 : this.table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ColumnVariableParser))
			return false;
		ColumnVariableParser other = (ColumnVariableParser) obj;
		if (this.table == null)
		{
			if (other.table != null)
				return false;
		}
		else if (!this.table.equals(other.table))
			return false;
		return true;
	}

	private Column getColumn(String variableName)
	{
		try
		{
			return this.table.getColumnWithVariableName(variableName);
		}
		catch (Table.NoSuchColumnException e)
		{
			throw new VariableGenerationFailedException("Can't find column for variable '" + 
					variableName + "' from table " + this.table, e);
		}
	}
}
