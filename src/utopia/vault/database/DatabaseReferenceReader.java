package utopia.vault.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import utopia.flow.structure.ImmutableList;
import utopia.vault.generics.Column;
import utopia.vault.generics.Table;
import utopia.vault.generics.TableInitialisationException;
import utopia.vault.generics.TableReference;
import utopia.vault.generics.TableReferenceReader;

/**
 * This object is able to read table references from the database
 * @author Mikko Hilpinen
 * @since 17.7.2016
 */
public class DatabaseReferenceReader implements TableReferenceReader
{
	// IMPLEMENTED METHODS	-----------------
	
	@SuppressWarnings("resource") // Resources are closed, but this is done through database class
	@Override
	public ImmutableList<TableReference> getReferencesBetween(Table from, Table to) throws TableInitialisationException
	{
		Database connection = new Database("INFORMATION_SCHEMA");
		PreparedStatement statement = null;
		ResultSet results = null;
		
		try
		{
			// Parses the sql
			StringBuilder sql = new StringBuilder();
			
			// Selects referenced and referencing column names
			sql.append("SELECT COLUMN_NAME, REFERENCED_COLUMN_NAME FROM KEY_COLUMN_USAGE");
			// Tables and databases must match
			sql.append(" WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME  = ?");
			
			// Creates the statement and inserts the values
			statement = connection.getPreparedStatement(sql.toString());
			statement.setString(1, from.getDatabaseName());
			statement.setString(2, from.getName());
			statement.setString(3, to.getDatabaseName());
			statement.setString(4, to.getName());
			
			// Executes the query
			results = statement.executeQuery();
			
			// and parses the results
			List<TableReference> references = new ArrayList<>();
			while (results.next())
			{
				Column fromColumn = from.findColumnWithColumnName(results.getString("COLUMN_NAME"));
				Column toColumn = to.findColumnWithColumnName(results.getString("REFERENCED_COLUMN_NAME"));
				references.add(new TableReference(fromColumn, toColumn));
			}
			
			return ImmutableList.of(references);
		}
		catch (DatabaseUnavailableException | SQLException e)
		{
			throw new TableInitialisationException("Failed to read table references from the database", e);
		}
		finally
		{
			// Closes the resources
			Database.closeResults(results);
			Database.closeStatement(statement);
			connection.closeConnection();
		}
	}
}
