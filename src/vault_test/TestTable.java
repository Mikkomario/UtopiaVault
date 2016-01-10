package vault_test;

import java.sql.SQLException;
import java.util.List;

import vault_database_old.AttributeNameMapping;
import vault_database_old.DatabaseTable;
import vault_database_old.DatabaseUnavailableException;
import vault_generics.AfterLastUnderLineRule;

/**
 * Table(s) used for testing
 * @author Mikko Hilpinen
 * @since 29.9.2015
 */
public enum TestTable implements DatabaseTable
{
	/**
	 * test_id (Auto-increment) | test_name | test_additional
	 */
	DEFAULT;
	
	
	// ATTRIBUTES	------------------
	
	private static List<Column> columnInfo = null;
	private static AttributeNameMapping mapping = null;
	
	
	// IMPLEMENTED METHODS	----------

	@Override
	public boolean usesAutoIncrementIndexing()
	{
		return true;
	}

	@Override
	public String getDatabaseName()
	{
		return "test_db";
	}

	@Override
	public String getTableName()
	{
		return "test";
	}

	@Override
	public List<String> getColumnNames()
	{
		return DatabaseTable.getColumnNamesFromColumnInfo(getColumnInfo());
	}

	@Override
	public Column getPrimaryColumn()
	{
		return DatabaseTable.findPrimaryColumnInfo(getColumnInfo());
	}

	@Override
	public List<Column> getColumnInfo()
	{
		if (columnInfo == null)
		{
			try
			{
				columnInfo = DatabaseTable.readColumnInfoFromDatabase(this);
			}
			catch (DatabaseUnavailableException | SQLException e)
			{
				System.out.println("Couldn't read column info");
				e.printStackTrace();
			}
		}
		
		return columnInfo;
	}

	@Override
	public AttributeNameMapping getAttributeNameMapping()
	{
		if (mapping == null)
		{
			mapping = new AttributeNameMapping();
			mapping.addRule(AfterLastUnderLineRule.getInstance());
			mapping.addMappingForEachColumnWherePossible(getColumnInfo());
		}
		
		return mapping;
	}
}
