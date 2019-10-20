package utopia.java.vault.test;

import utopia.java.flow.generics.Value;
import utopia.java.flow.structure.ImmutableList;
import utopia.java.flow.structure.Option;
import utopia.java.vault.database.InCondition;
import utopia.java.vault.generics.BasicSqlDataType;
import utopia.java.vault.generics.Column;
import utopia.java.vault.generics.ColumnInitialiser;
import utopia.java.vault.generics.ColumnNameIsVariableNameRule;
import utopia.java.vault.generics.Table;
import utopia.java.vault.generics.TableInitialisationException;
import utopia.java.vault.generics.VariableNameMapping;
import utopia.java.vault.generics.VariableNameMapping.NoVariableForColumnException;

/**
 * This class makes a simple test for the in-condition
 * @author Mikko Hilpinen
 * @since 28.9.2018
 */
public class InConditionTest
{
	@SuppressWarnings("javadoc")
	public static void main(String[] args)
	{
		BasicSqlDataType.initialise();
		
		ColumnInitialiser initializer = table -> {
			try
			{
				Column c1 = new Column(table, "c1", BasicSqlDataType.INT, false, true, true, Value.EMPTY);
				Column c2 = new Column(table, "c2", BasicSqlDataType.VARCHAR, false, false, false, Value.EMPTY);

				return ImmutableList.withValues(c1, c2);
			}
			catch (NoVariableForColumnException e)
			{
				throw new TableInitialisationException("Column creation failed", e);
			}
		};
		
		VariableNameMapping mapping = new VariableNameMapping();
		mapping.addRule(ColumnNameIsVariableNameRule.getInstance());
		
		Table table = new Table("db", "table", mapping, initializer, Option.none());
		
		Column column = table.getColumnWithVariableName("c2");
		InCondition where = new InCondition(column, ImmutableList.withValues("a", "b", "c").map(Value::of));
		
		System.out.println(where);
		System.out.println(where.toSql());
	}
}
