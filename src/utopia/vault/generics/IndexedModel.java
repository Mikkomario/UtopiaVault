package utopia.vault.generics;

import utopia.flow.generics.Value;
import utopia.flow.structure.Option;
import utopia.vault.generics.Table.NoSuchColumnException;

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
	 * @throws NoSuchColumnException If the model didn't have an index (column)
	 */
	public default Value getIndex() throws NoSuchColumnException
	{
		return getIndexOption().getOrFail(() -> new NoSuchColumnException("No index available"));
	}
}
