package utopia.vault.generics;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utopia.flow.generics.BasicDataType;
import utopia.flow.generics.Conversion;
import utopia.flow.generics.ConversionReliability;
import utopia.flow.generics.DataType;
import utopia.flow.generics.Value;
import utopia.flow.generics.ValueParser;
import utopia.flow.structure.Pair;

/**
 * This parser is able to parse values between the basic data types and the sql data types
 * @author Mikko Hilpinen
 * @since 8.2.2016
 */
public class SqlValueParser implements ValueParser
{
	// ATTRIBUTES	---------------
	
	private static SqlValueParser instance = null;
	
	private List<Conversion> conversions = new ArrayList<>();
	private List<Pair<DataType, DataType>> wrapable = new ArrayList<>();
	
	
	// CONSTRUCTOR	---------------
	
	private SqlValueParser()
	{
		// Creates the supported wraps
		addWrap(BasicDataType.STRING, BasicSqlDataType.VARCHAR);
		addWrap(BasicDataType.BOOLEAN, BasicSqlDataType.BOOLEAN);
		addWrap(BasicDataType.INTEGER, BasicSqlDataType.INT);
		addWrap(BasicDataType.LONG, BasicSqlDataType.BIGINT);
		addWrap(BasicDataType.DOUBLE, BasicSqlDataType.DOUBLE);
		addWrap(BasicDataType.FLOAT, BasicSqlDataType.FLOAT);
		
		// Creates the conversions
		this.conversions.add(new Conversion(BasicDataType.DATE, BasicSqlDataType.DATE, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicSqlDataType.DATE, BasicDataType.DATE, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicDataType.TIME, BasicSqlDataType.TIME, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicSqlDataType.TIME, BasicDataType.TIME, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicDataType.DATETIME, BasicSqlDataType.TIMESTAMP, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicSqlDataType.TIMESTAMP, BasicDataType.DATETIME, 
				ConversionReliability.PERFECT));
		this.conversions.add(new Conversion(BasicDataType.STRING, BasicSqlDataType.TIMESTAMP, 
				ConversionReliability.DANGEROUS));
		this.conversions.add(new Conversion(BasicDataType.STRING, BasicDataType.DATETIME, 
				ConversionReliability.DANGEROUS));
		
		for (Pair<DataType, DataType> wrap : this.wrapable)
		{
			this.conversions.add(new Conversion(wrap.getFirst(), wrap.getSecond(), 
					ConversionReliability.NO_CONVERSION));
			this.conversions.add(new Conversion(wrap.getSecond(), wrap.getFirst(), 
					ConversionReliability.NO_CONVERSION));
		}
	}
	
	/**
	 * @return The static parser instance
	 */
	public static SqlValueParser getInstance()
	{
		if (instance == null)
			instance = new SqlValueParser();
		return instance;
	}
	
	
	// IMPLEMENTED METHODS	-------

	@Override
	public Value cast(Value value, DataType to) throws ValueParseException
	{
		DataType from = value.getType();
		
		if (from.equals(to))
			return value;
		// Null stays null
		if (value.isNull())
			return Value.NullValue(to);
		
		// Checks if the values are simply wrapable
		for (Pair<DataType, DataType> pair : this.wrapable)
		{
			if ((pair.getFirst().equals(from) && pair.getSecond().equals(to)) || 
					(pair.getFirst().equals(to) && pair.getSecond().equals(from)))
				return wrap(value, to);
		}
		
		// Can also cast sql dates to local dates
		if (to.equals(BasicDataType.DATE))
		{
			if (from.equals(BasicSqlDataType.DATE))
				return Value.Date(BasicSqlDataType.valueToDate(value).toLocalDate());
		}
		else if (to.equals(BasicSqlDataType.DATE))
		{
			if (from.equals(BasicDataType.DATE))
				return BasicSqlDataType.Date(Date.valueOf(value.toLocalDate()));
		}
		else if (to.equals(BasicDataType.TIME))
		{
			if (from.equals(BasicSqlDataType.TIME))
				return Value.Time(BasicSqlDataType.valueToTime(value).toLocalTime());
		}
		else if (to.equals(BasicSqlDataType.TIME))
		{
			if (from.equals(BasicDataType.TIME))
				return BasicSqlDataType.Time(Time.valueOf(value.toLocalTime()));
		}
		// Datetimes may be parsed from strings through timestamp if the default format fails
		else if (to.equals(BasicDataType.DATETIME))
		{
			if (from.equals(BasicSqlDataType.TIMESTAMP))
				return Value.DateTime(BasicSqlDataType.valueToTimeStamp(value).toLocalDateTime());
			else if (from.equals(BasicDataType.STRING))
			{
				String stringVal = value.toString();
				try
				{
					return Value.DateTime(LocalDateTime.parse(stringVal));
				}
				catch (DateTimeParseException e)
				{
					try
					{
						return Value.DateTime(Timestamp.valueOf(stringVal).toLocalDateTime());
					}
					catch (IllegalArgumentException e1)
					{
						throw new ValueParseException(value, from, to, e);
					}
				}
			}
		}
		// Timestamps can also be parsed from strings
		else if (to.equals(BasicSqlDataType.TIMESTAMP))
		{
			if (from.equals(BasicDataType.DATETIME))
				return BasicSqlDataType.Timestamp(Timestamp.valueOf(value.toLocalDateTime()));
			else if (from.equals(BasicDataType.STRING))
			{
				String stringVal = value.toString();
				if (stringVal.equalsIgnoreCase("CURRENT_TIMESTAMP"))
					return new CurrentTimestamp();
				else
				{
					try
					{
						return BasicSqlDataType.Timestamp(Timestamp.valueOf(stringVal));
						
					}
					catch (IllegalArgumentException e)
					{
						try
						{
							return BasicSqlDataType.Timestamp(Timestamp.valueOf(LocalDateTime.parse(
									stringVal)));
						}
						catch (DateTimeParseException e2)
						{
							throw new ValueParseException(value, from, to, e);
						}
					}
				}
			}
		}
		
		throw new ValueParseException(value, to);
	}

	@Override
	public Collection<? extends Conversion> getConversions()
	{
		return this.conversions;
	}
	
	
	// OTHER METHODS	---------
	
	private static Value wrap(Value value, DataType to)
	{
		return new Value(value.getObjectValue(), to);
	}
	
	private void addWrap(DataType first, DataType second)
	{
		this.wrapable.add(new Pair<>(first, second));
	}
}
