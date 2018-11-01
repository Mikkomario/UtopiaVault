package utopia.vault.database;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import utopia.flow.async.Completion;
import utopia.flow.async.Volatile;
import utopia.flow.structure.ImmutableList;
import utopia.flow.structure.ImmutableMap;
import utopia.flow.structure.ListBuilder;
import utopia.flow.structure.Option;
import utopia.flow.structure.Pair;
import utopia.flow.util.WaitUtils;

/**
 * ConnectionManagers are used for handling shared connections
 * @author Mikko Hilpinen
 * @since 1.11.2018
 */
public class ConnectionManager
{
	// ATTRIBUTES	--------------------
	
	// (connection amount, max clients per connection), Ordered by connection amount.
	private ImmutableList<Pair<Integer, Integer>> maxClientThresholds;
	private Volatile<ImmutableList<ReusableConnection>> connections = new Volatile<>(ImmutableList.empty());
	
	private Duration connectionKeepAlive;
	private Volatile<Completion> timeoutCompletion = new Volatile<>(Completion.fulfilled());
	private Object waitLock = new String();
	
	
	// CONSTRUCTOR	--------------------
	
	/**
	 * Creates a new connection manager
	 * @param connectionKeepAlive The maximum idle duration of a connection before it is closed
	 * @param maxClientThresholds Thresholds for each client amount update (connection amount -> max client amount)
	 */
	public ConnectionManager(Duration connectionKeepAlive, ImmutableMap<Integer, Integer> maxClientThresholds)
	{
		this.connectionKeepAlive = connectionKeepAlive;
		this.maxClientThresholds = maxClientThresholds.toList().sortedBy(p -> p.getFirst());
	}
	
	/**
	 * Creates a new connection manager
	 * @param maxConnections The total maximum amount of connections
	 * @param clientsPerConnectionCap The maximum amount of clients when connections at maximum
	 * @param connectionKeepAlive The maximum idle duration of a connection before it is closed
	 */
	public ConnectionManager(int maxConnections, int clientsPerConnectionCap, Duration connectionKeepAlive)
	{
		this.connectionKeepAlive = connectionKeepAlive;
		
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
		
		this.maxClientThresholds = buffer.build();
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
			// Finds the maximum clients per connection treshold
			int maxClients = getMaxClientsPerConnection(all.size());
			
			// Returns the first reusable connection, if no such connection exists, creates a new connection
			// Tries to use the connection with least clients
			Option<ReusableConnection> reusable = all.minBy(c -> c.getCurrentClientAmount()).filter(
					c -> c.tryJoin(maxClients));
			
			if (reusable.isDefined())
				return new Pair<>(reusable.get(), all);
			else
			{
				ReusableConnection newConnection = new ReusableConnection(this::closeUnusedConnections);
				return new Pair<>(newConnection, all.plus(newConnection));
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
	
	private void closeUnusedConnections()
	{
		// Makes sure connection closing is active
		timeoutCompletion.update(old -> 
		{
			// Only a single close thread is active at a time
			if (old.isFulfilled())
			{
				return Completion.ofAsynchronous(() -> 
				{
					Option<Instant> nextWait = Option.some(Instant.now().plus(connectionKeepAlive));
					
					// Closes connections as long as they are queued to be closed
					while (nextWait.isDefined())
					{
						WaitUtils.waitUntil(nextWait.get(), waitLock);
						
						// Updates the connections list and determines next close time
						nextWait = connections.pop(all -> 
						{
							// Keeps the connections that are still open
							Instant closeThreshold = Instant.now().minus(connectionKeepAlive);
							ImmutableMap<Boolean, ImmutableList<ReusableConnection>> closedAndOpen = all.divideBy(
									c -> c.isOpen(closeThreshold));
							
							ImmutableList<ReusableConnection> open = closedAndOpen.get(true);
							
							// Terminates connections on closed items
							closedAndOpen.get(false).forEach(c -> c.connection.close());
							
							// Calculates the time when the next connection will be closed
							Option<Instant> lastLeaveTime = open.filter(c -> !c.isInUse()).mapMin(c -> c.lastLeaveTime);
							
							return new Pair<>(lastLeaveTime.map(t -> t.plus(connectionKeepAlive)), open);
						});
					}
				});
			}
			else
				return old;
		});
	}
	
	private int getMaxClientsPerConnection(int openConnections)
	{
		if (maxClientThresholds.isEmpty())
			return 1;
		else
		{
			// Finds the first treshold that hasn't been reached and uses that connection amount
			return maxClientThresholds.find(p -> p.getFirst() >= openConnections).getOrElse(
					() -> maxClientThresholds.last().get()).getSecond();
		}
	}

	
	// NESTED CLASSES	--------------------
	
	private static class ReusableConnection
	{
		// ATTRIBUTES	--------------------
		
		private Database connection = new Database();
		private Volatile<Integer> clients = new Volatile<>(1);
		private Instant lastLeaveTime = Instant.now();
		
		private Runnable onIdleOperation;
		
		
		// CONSTRUCTOR	--------------------
		
		public ReusableConnection(Runnable onIdleOperation)
		{
			this.onIdleOperation = onIdleOperation;
		}
		
		
		// OTHER	------------------------
		
		public int getCurrentClientAmount()
		{
			return clients.get();
		}
		
		public boolean isInUse()
		{
			return getCurrentClientAmount() > 0;
		}
		
		public boolean isOpen(Instant closeThreshold)
		{
			return isInUse() || lastLeaveTime.isAfter(closeThreshold);
		}
		
		public boolean tryJoin(int maxCapacity)
		{
			return clients.pop(current -> 
			{
				if (current >= maxCapacity)
					return new Pair<>(false, current);
				else
					return new Pair<>(true, current + 1);
			});
		}
		
		public void leave()
		{
			lastLeaveTime = Instant.now();
			clients.update(current -> 
			{
				if (current == 1)
					onIdleOperation.run();
				return current - 1;
			});
		}
	}
}
