package utopia.vault.database;

import java.util.function.Consumer;

import utopia.flow.async.Volatile;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.ImmutableMap;
import utopia.flow.structure.ListBuilder;
import utopia.flow.structure.Option;
import utopia.flow.structure.Pair;

/**
 * ConnectionManagers are used for handling shared connections
 * @author Mikko Hilpinen
 * @since 1.11.2018
 */
public class ConnectionManager
{
	// ATTRIBUTES	--------------------
	
	// (connection amount, max clients per connection), Ordered by connection amount.
	private ImmutableList<Pair<Integer, Integer>> maxClientTresholds;
	private Volatile<ImmutableList<ReusableConnection>> connections = new Volatile<>(ImmutableList.empty());
	
	
	// CONSTRUCTOR	--------------------
	
	/**
	 * Creates a new connection manager
	 * @param maxClientTresholds Tresholds for each client amount update (connection amount -> max client amount)
	 */
	public ConnectionManager(ImmutableMap<Integer, Integer> maxClientTresholds)
	{
		this.maxClientTresholds = maxClientTresholds.toList().sortedBy(p -> p.getFirst());
	}
	
	/**
	 * Creates a new connection manager
	 * @param maxConnections The total maximum amount of connections
	 * @param clientsPerConnectionCap The maximum amount of clients when connections at maximum
	 */
	public ConnectionManager(int maxConnections, int clientsPerConnectionCap)
	{
		int currentMax = 1;
		int start = 0;
		
		// Creates a new max connection treshold list
		ListBuilder<Pair<Integer, Integer>> buffer = new ListBuilder<>();
		
		// Uses halving algorithm (for example getting 0 to 100: 50, 75, 87, 93, 96, 98, 99)
		while (currentMax < clientsPerConnectionCap - 1)
		{
			int length = (maxConnections - start) / 2;
			if (length <= 0)
				break;
			
			buffer.add(new Pair<>(currentMax, start + length));
			
			currentMax ++;
			start += length;
		}
		
		buffer.add(new Pair<>(maxConnections, clientsPerConnectionCap));
		
		this.maxClientTresholds = buffer.build();
	}
	
	
	// OTHER	------------------------
	
	/**
	 * Provides access to a connection for a client
	 * @param client A client function that uses the provided connection
	 */
	public void getConnection(Consumer<? super Database> client)
	{
		ReusableConnection connection = connections.pop(all -> 
		{
			// Removes closed connections
			ImmutableList<ReusableConnection> open = all.filter(c -> c.isOpen());
			
			// Finds the maximum clients per connection treshold
			int maxClients = getMaxClientsPerConnection(open.size());
			
			// Returns the first reusable connection, if no such connection exists, creates a new connection
			Option<ReusableConnection> reusable = open.find(c -> c.tryJoin(maxClients));
			
			if (reusable.isDefined())
				return new Pair<>(reusable.get(), open);
			else
			{
				ReusableConnection newConnection = new ReusableConnection();
				return new Pair<>(newConnection, open.plus(newConnection));
			}
		});
		
		try
		{
			client.accept(connection.connection);
		}
		finally
		{
			connection.leave();
		}
	}
	
	private int getMaxClientsPerConnection(int openConnections)
	{
		if (maxClientTresholds.isEmpty())
			return 1;
		else
		{
			// Finds the first treshold that hasn't been reached and uses that connection amount
			return maxClientTresholds.find(p -> p.getFirst() >= openConnections).getOrElse(
					() -> maxClientTresholds.last().get()).getSecond();
		}
	}

	
	// NESTED CLASSES	--------------------
	
	private static class ReusableConnection
	{
		// ATTRIBUTES	--------------------
		
		private Database connection = new Database();
		private Volatile<Integer> clients = new Volatile<>(1);
		
		
		// OTHER	------------------------
		
		public boolean isOpen()
		{
			return clients.get() > 0;
		}
		
		public boolean tryJoin(int maxCapacity)
		{
			return clients.pop(current -> 
			{
				// Closed connections won't be used anymore
				if (current > 0 || current >= maxCapacity)
					return new Pair<>(false, current);
				else
					return new Pair<>(true, current + 1);
			});
		}
		
		public void leave()
		{
			clients.update(current -> 
			{
				if (current == 1)
					connection.close();
				return current - 1;
			});
		}
	}
}
