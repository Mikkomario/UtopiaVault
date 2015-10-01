package vault_test;

import java.sql.SQLException;
import java.util.List;

import vault_database.AfterLastUnderLineRule;
import vault_database.AttributeNameMapping;
import vault_database.DatabaseTable;
import vault_database.DatabaseUnavailableException;

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
	
	private static List<ColumnInfo> columnInfo = null;
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
	public ColumnInfo getPrimaryColumn()
	{
		return DatabaseTable.findPrimaryColumnInfo(getColumnInfo());
	}

	@Override
	public List<ColumnInfo> getColumnInfo()
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
