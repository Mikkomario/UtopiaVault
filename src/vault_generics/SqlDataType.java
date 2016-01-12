package vault_generics;

import java.sql.Types;

import flow_generics.BasicDataType;
import flow_generics.DataType;
import flow_generics.DataTypes;
import flow_generics.SubTypeSet;
import flow_generics.Value;

/**
 * These data types work like any other, except that they can be used in sql operations
 * @author Mikko Hilpinen
 * @since 19.18.2016
 */
public interface SqlDataType extends DataType
{
	/**
	 * @return The sql type of this data type
	 * @see Types
	 */
	public int getSqlType();
	
	/**
	 * Casts a general data type to an instance of sql data type. The casting works if the 
	 * type is already a sql data type or if the type is a basic data type identical to one 
	 * of the sql types
	 * @param type A data type
	 * @return The sql data type equivalent of the provided data type. Null if the type 
	 * is not an sql data type or comparable
	 */
	public static SqlDataType castToSqlDataType(DataType type)
	{
		if (type instanceof SqlDataType)
			return (SqlDataType) type;
		else
		{
			if (type.equals(BasicDataType.STRING))
				return SimpleSqlDataType.VARCHAR;
			else if (type.equals(BasicDataType.BOOLEAN))
				return SimpleSqlDataType.BOOLEAN;
			else if (type.equals(BasicDataType.INTEGER))
				return SimpleSqlDataType.INT;
			else if (type.equals(BasicDataType.LONG))
				return SimpleSqlDataType.BIGINT;
			else if (type.equals(BasicDataType.DOUBLE))
				return SimpleSqlDataType.DOUBLE;
			
			return null;
		}
	}
	
	/**
	 * @return All the sql types as a type set
	 */
	public static SubTypeSet getSqlTypes()
	{
		SubTypeSet types = new SubTypeSet();
		for (DataType anyType : DataTypes.getInstance().getIntroducedDataTypes())
		{
			if (anyType instanceof SqlDataType)
				types.add(anyType);
		}
		
		return types;
	}
	
	/**
	 * Casts a value to one of the sql-compatible data types
	 * @param value The value that is casted
	 * @return The casted value
	 */
	public static Value castToSqlType(Value value)
	{
		return DataTypes.getInstance().cast(value, getSqlTypes());
	}
}
