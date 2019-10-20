package utopia.java.vault.generics;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import utopia.java.flow.generics.Value;

/**
 * Current timestamp is a value that always returns the current time
 * @author Mikko Hilpinen
 * @since 23.2.2016
 */
public class CurrentTimestamp extends Value
{
	// CONSTRUCTOR	--------------
	
	/**
	 * Creates a new current timestamp value
	 */
	public CurrentTimestamp()
	{
		super(null, BasicSqlDataType.TIMESTAMP);
	}

	
	// IMPLEMENTED METHODS	------
	
	/**
	 * Current timestamp always returns the current time
	 */
	@Override
	public Object getObjectValue()
	{
		return Timestamp.valueOf(LocalDateTime.now());
	}
	
	@Override
	public String toString()
	{
		return "CURRENT_TIMESTAMP";
	}
}
