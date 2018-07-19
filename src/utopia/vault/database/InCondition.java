package utopia.vault.database;

import utopia.flow.generics.DataType;
import utopia.flow.generics.Value;
import utopia.flow.structure.Either;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.Option;
import utopia.vault.generics.Column;

/**
 * This condition searches for rows where certain column has value in certain group or where 
 * a value can be found within certain column group
 * @author Mikko Hilpinen
 * @since 21.7.2016
 */
public class InCondition extends Condition
{
	// ATTRIBUTES	------------------
	
	private ImmutableList<Either<Column, Value>> parts;
	
	
	// CONSTRUCTOR	------------------
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param firstValue The first possible value
	 * @param secondValue The second possible value
	 * @param moreValues More possible values
	 */
	public InCondition(Column column, Value firstValue, Value secondValue, Value... moreValues)
	{
		ImmutableList<Either<Column, Value>> valueParts = ImmutableList.withValues(firstValue, secondValue, 
				moreValues).map(Either::right);
		
		this.parts = valueParts.prepend(Either.left(column));
	}
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param inValues The accepted values
	 */
	public InCondition(Column column, ImmutableList<Value> inValues)
	{
		ImmutableList<Either<Column, Value>> valueParts = inValues.map(Either::right);
		
		this.parts = valueParts.prepend(Either.left(column));
	}
	
	/**
	 * Creates a new condition
	 * @param value The searched value
	 * @param firstColumn The first possible column
	 * @param secondColumn The second possible column
	 * @param moreColumns More possible columns
	 */
	public InCondition(Value value, Column firstColumn, Column secondColumn, Column... moreColumns)
	{
		ImmutableList<Either<Column, Value>> columnParts = ImmutableList.withValues(firstColumn, secondColumn, 
				moreColumns).map(Either::left);
		
		this.parts = columnParts.prepend(Either.right(value));
	}
	
	/**
	 * Creates a new condition
	 * @param value The searched value
	 * @param inColumns The columns the value is searched from
	 */
	public InCondition(Value value, ImmutableList<Column> inColumns)
	{
		ImmutableList<Either<Column, Value>> columnParts = inColumns.map(Either::left);
		
		this.parts = columnParts.prepend(Either.right(value));
	}
	
	/**
	 * Creates a new condition
	 * @param column The checked column
	 * @param firstColumn The first matching column option
	 * @param secondColumn The second matching column option
	 * @param moreColumns more matching column options
	 */
	public InCondition(Column column, Column firstColumn, Column secondColumn, Column... moreColumns)
	{
		ImmutableList<Either<Column, Value>> inParts = ImmutableList.withValues(firstColumn, secondColumn, 
				moreColumns).map(Either::left);
		this.parts = inParts.prepend(Either.left(column));
	}
	
	
	// IMPLEMENTED METHODS	-------------
	
	@Override
	public ImmutableList<Value> getValues()
	{
		// May cast the values to a certain data type
		Option<DataType> type = this.parts.headOption().flatMap(p -> p.left()).map(c -> c.getType());
		ImmutableList<Value> values = this.parts.flatMap(e -> e.right());
		
		if (type.isDefined())
			return values.map(v -> v.castTo(type.get()));
		else
			return values;
	}

	@Override
	public String toSql()
	{
		// If there is 'in' set, cannot be true
		if (this.parts.size() <= 1)
			return "0";
		else
		{
			StringBuilder sql = new StringBuilder();
			sql.append(partToSql(this.parts.head()));
			sql.append(" IN (");
			sql.append(this.parts.map(InCondition::partToSql).reduce((total, part) -> total + ", " + part));
			sql.append(")");
			
			return sql.toString();
		}
	}

	
	// OTHER METHODS	----------------
	
	private static String partToSql(Either<Column, Value> part)
	{
		return part.toValue(c -> c.getColumnNameWithTable(), v -> "?");
	}
}
