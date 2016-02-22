package utopia.vault.test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import utopia.flow.generics.DataType;
import utopia.flow.generics.DataTypeException;
import utopia.flow.generics.DataTypes;
import utopia.flow.generics.Value;
import utopia.vault.generics.BasicSqlDataType;
import utopia.vault.generics.SqlDataType;

/**
 * This test tries to convert values from a basic data type into sql compatible data types
 * @author Mikko Hilpinen
 * @since 22.2.2016
 */
public class SqlDataTypeConversionTest
{
	// MAIN METHOD	--------------
	
	/**
	 * Tests sql data type conversion
	 * @param args not used
	 */
	public static void main(String[] args)
	{
		BasicSqlDataType.initialise();
		
		cast(Value.String("string"), BasicSqlDataType.VARCHAR);
		cast(Value.Integer(15), BasicSqlDataType.INT);
		cast(Value.Long(218375878247823l), BasicSqlDataType.BIGINT);
		cast(Value.Double(1.234), BasicSqlDataType.DOUBLE);
		cast(Value.Boolean(true), BasicSqlDataType.BOOLEAN);
		cast(Value.Date(LocalDate.now()), BasicSqlDataType.DATE);
		cast(Value.DateTime(LocalDateTime.now()), BasicSqlDataType.TIMESTAMP);
		cast(Value.Time(LocalTime.now()), BasicSqlDataType.TIME);
		
		test(Value.String("string"));
		test(Value.Integer(15));
		test(Value.Long(1233859348848493939l));
		test(Value.Double(1.2345));
		test(Value.Boolean(true));
		test(Value.Date(LocalDate.now()));
		test(Value.DateTime(LocalDateTime.now()));
		test(Value.Time(LocalTime.now()));
	}
	
	private static void cast(Value value, DataType type)
	{
		System.out.println("Casting " + value.getDescription() + " to " + type);
		System.out.println("-> " + value.castTo(type));
		System.out.println("Reliability: " + 
				DataTypes.getInstance().getConversionReliability(value.getType(), type));
	}
	
	private static void test(Value value)
	{
		try
		{ 
			System.out.println("Casting " + value.getDescription());
			System.out.println("-> " + SqlDataType.castToSqlType(value).getDescription());
		}
		catch (DataTypeException e)
		{
			e.printStackTrace();
		}
	}
}
