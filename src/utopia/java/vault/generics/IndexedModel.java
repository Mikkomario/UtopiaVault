package utopia.java.vault.generics;

import utopia.java.flow.generics.Value;
import utopia.java.flow.structure.Option;

/**
 * This interface is implemented by model implementations that support the use of indices
 * @author Mikko Hilpinen
 * @since 17.4.2018
 */
public interface IndexedModel
{
	// ABSTRACT	--------------------
	
	/**
	 * @return This model's index in value format
	 */
	public Option<Value> getIndexOption();
	
	/**
	 * @param index The new index for this model
	 */
	public void setIndex(Value index);
	
	
	// OTHER METHODS	-------------
	
	/**
	 * @return The index for this model
	 * @throws Table.NoSuchColumnException If the model didn't have an index (column)
	 */
	public default Value getIndex() throws Table.NoSuchColumnException
	{
		return getIndexOption().getOrFail(() -> new Table.NoSuchColumnException("No index available"));
	}
}
