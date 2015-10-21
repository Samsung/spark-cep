package redis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class RedisConnectionPool extends Pool<ShardedJedis> {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnectionPool.class);
	protected GenericObjectPoolConfig poolConfig;
    protected int timeout = Protocol.DEFAULT_TIMEOUT;
    private int sentinelRetry = 0;
    private int sentinelMaxRetry = 10;
    protected String password;
    protected int database = Protocol.DEFAULT_DATABASE;
    protected Map<String, MasterListener> masterListeners = new HashMap<String, MasterListener>();
    private volatile List<HostAndPort> currentHostMasters;
    
    // pool settings
	private static final int CONN_TIMEOUT_MILLIS = 5000;
	private static final int MAX_TOTAL_RESOURCES = 50;
	private static final int MAX_IDLE_RESOURCES = 20;
	private static final int MAX_WAIT_RESOURCE_MILLIS = 100;
	
	// singleton concurrent hash map - database number is the key
	private static Map<Integer, RedisConnectionPool> instanceMap = new ConcurrentHashMap<Integer, RedisConnectionPool>();
    
	List<String> shardList;
	Set<String> sentinelSet;
	
	// get instance from instance Map
	public static RedisConnectionPool getInstance(String shards, String sentinels, int database) {
		if (! instanceMap.containsKey(database)) {
	    	GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxTotal(MAX_TOTAL_RESOURCES);
			config.setMaxIdle(MAX_IDLE_RESOURCES);
			config.setMaxWaitMillis(MAX_WAIT_RESOURCE_MILLIS);
			
			instanceMap.put(database, new RedisConnectionPool(shards, sentinels,
				config, CONN_TIMEOUT_MILLIS, null, database));
		}
		return instanceMap.get(database);
	}

    public RedisConnectionPool(String shards, String sentinels,
	    final GenericObjectPoolConfig poolConfig, int timeout,
	    final String password, final int database) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;

		shardList = new ArrayList<String>();
		for (String shard : shards.split(",")) {
			shardList.add(shard.trim());
		}
		sentinelSet = new HashSet<String>();
		for (String sentinel : sentinels.split(",")) {
			sentinelSet.add(sentinel.trim());
		}
    }
    
    public synchronized void init() {
    	List<HostAndPort> masterList = initSentinels(sentinelSet, shardList);
		initPool(masterList);
    }

    public void destroy() {
		for (MasterListener m : masterListeners.values()) {
		    m.shutdown();
		}
		super.destroy();
    }
    
    public List<HostAndPort> getCurrentHostMaster() {
    	return currentHostMasters;
    }

    private void initPool(List<HostAndPort> masters) {
    	if (!equals(currentHostMasters, masters)) {
    		StringBuffer sb = new StringBuffer();
    		for (HostAndPort master : masters) {
    			sb.append(master.toString());
    			sb.append(" ");
    		}
    		LOGGER.info("Created ShardedJedisPool to master at [" + sb.toString() + "]");
    		List<JedisShardInfo> shardMasters = makeShardInfoList(masters);
    		initPool(poolConfig, new ShardedJedisFactory(shardMasters, Hashing.MURMUR_HASH, null, database));
    		currentHostMasters = masters;
    	}
    }
    
    private boolean equals(List<HostAndPort> currentShardMasters, List<HostAndPort> shardMasters) {
    	if (currentShardMasters != null && shardMasters != null) {
    		if (currentShardMasters.size() == shardMasters.size()) {
    			for (int i = 0; i < currentShardMasters.size(); i++) {
    				if (!currentShardMasters.get(i).equals(shardMasters.get(i))) return false;
    			}
    			return true;
    		}
    	}
		return false;
	}

	private List<JedisShardInfo> makeShardInfoList(List<HostAndPort> masters) {
		List<JedisShardInfo> shardMasters = new ArrayList<JedisShardInfo>();
		for (HostAndPort master : masters) {
			JedisShardInfo jedisShardInfo = new JedisShardInfo(master.getHost(), master.getPort(), timeout);
			jedisShardInfo.setPassword(password);
			
			shardMasters.add(jedisShardInfo);
		}
		return shardMasters;
	}

	public Jedis getNewJedis(String host, int port) {
		return new Jedis(host, port);
	}
	
	public void setSentinelMaxRetry(int sentinelMaxRetry) {
		this.sentinelMaxRetry = sentinelMaxRetry;
	}

	private List<HostAndPort> initSentinels(Set<String> sentinels, final List<String> masters) {
    	Map<String, HostAndPort> masterMap = new TreeMap<String, HostAndPort>();
    	LOGGER.info("Trying to find all master from available Sentinels...");
	    
    	HostAndPort master = null;
    	boolean fetched = false;
    	
    	while (!fetched && sentinelRetry < sentinelMaxRetry) {
    		for (String sentinel : sentinels) {
				final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
				LOGGER.info("Connecting to Sentinel " + hap);
		
				try {
					Jedis jedis = getNewJedis(hap.getHost(), hap.getPort());
					List<Map<String, String>> mastersInfo = jedis.sentinelMasters();
					jedis.disconnect();
					
					for (String masterName : masters) {
						master = masterMap.get(masterName);
					    if (master == null) {
					    	for (Map<String, String> masterInfo : mastersInfo) {
					    		
					    		if (masterName.equals(masterInfo.get("name")) & !masterInfo.containsKey("o-down-time")) {
					    			List<String> hostAndPort = new ArrayList<String>();
					    			hostAndPort.add(masterInfo.get("ip"));
					    			hostAndPort.add(masterInfo.get("port"));
					    			
							    	if (hostAndPort != null && hostAndPort.size() > 0) {
							    		master = toHostAndPort(hostAndPort);
										LOGGER.info("Found Redis " + masterName + " master at " + master);
										masterMap.put(masterName, master);
										fetched = true;
							    	}
					    		}
					    	}
					    }
					}
					
				} catch (JedisConnectionException e) {
					LOGGER.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
				}
	    	}
	    	
	    	if (!fetched) {
	    		try {
	    			LOGGER.error("Cannot determine where "
	    				+ "masters are running... sleeping 1000ms, Will try again.");
					Thread.sleep(1000);
			    } catch (InterruptedException e) {
			    	e.printStackTrace();
			    }
	    		sentinelRetry++;
	    	}
    	}
	    	
    	// Try MAX_RETRY_SENTINEL times.
    	if (!fetched && sentinelRetry >= sentinelMaxRetry) {
    		LOGGER.error("All sentinels or redis databases down and try " + sentinelMaxRetry + " times, Abort.");
    		throw new JedisConnectionException("Cannot connect all !, Abort.");
    	}
	    
	    // All shards master must been accessed.
	    if (masters.size() != 0 && masterListeners.isEmpty()) {
	    	LOGGER.info("Starting Sentinel listeners...");
			for (String sentinel : sentinels) {
			    final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
	    		MasterListener masterListener = new MasterListener(masters, hap.getHost(), hap.getPort());
	    		masterListeners.put(sentinel, masterListener);
			    masterListener.start();
			}
	    }
	    
		return new ArrayList<HostAndPort>(masterMap.values());
    }
	
    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
    	String host = getMasterAddrByNameResult.get(0);
    	int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
    	
    	return new HostAndPort(host, port);
    }
    
    /**
     * PoolableObjectFactory custom impl.
     */
    public static class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
		private List<JedisShardInfo> shards;
		private Hashing algo;
		private Pattern keyTagPattern;
		private int database;
	
		public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern, int database) {
		    this.shards = shards;
		    this.algo = algo;
		    this.keyTagPattern = keyTagPattern;
		    this.database = database;
		}

		public PooledObject<ShardedJedis> makeObject() throws Exception {
		    ShardedJedis shardedJedis = new ShardedJedis(shards, algo, keyTagPattern);
		    for (Jedis jedis : shardedJedis.getAllShards()) {
				jedis.select(database);
		    }
		    return new DefaultPooledObject<ShardedJedis>(shardedJedis);
		}
	
		public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
		    final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
		    for (Jedis jedis : shardedJedis.getAllShards()) {
			try {
			    try {
				jedis.quit();
			    } catch (Exception e) {
			    	
			    }
			    jedis.disconnect();
			} catch (Exception e) {
	
			}
		    }
		}
	
		public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
		    try {
			ShardedJedis shardedJedis = pooledShardedJedis.getObject();
			for (Jedis jedis : shardedJedis.getAllShards()) {
			    if (!jedis.ping().equals("PONG")) {
				return false;
			    }
			}
			return true;
		    } catch (Exception ex) {
			return false;
		    }
		}
	
		public void activateObject(PooledObject<ShardedJedis> p) throws Exception {
		}
	
		public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {
	
		}
    }

    public class JedisPubSubAdapter extends JedisPubSub {
    	protected List<String> masters;
    	protected String host;
    	protected int port;
		
    	public JedisPubSubAdapter(List<String> masters, String host, int port) {
    		this.masters = masters;
 		    this.host = host;
 		    this.port = port;
    	}
    	
    	@Override
		public void onMessage(String channel, String message) {
			LOGGER.info("Sentinel " + host + ":" + port + " published: " + channel + " " + message);
			
			if ("+switch-master".equals(channel) || "+odown".equals(channel) || "-odown".equals(channel)) {
				init();
			}
		}
	
		@Override
		public void onPMessage(String pattern, String channel, String message) {
		}
	
		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
		}
	
		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
		}
	
		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
		}
	
		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
		}
    }

    public class MasterListener extends Thread {

		protected List<String> masters;
		protected String host;
		protected int port;
		protected long subscribeRetryWaitTimeMillis = 5000;
		protected Jedis jedis;
		protected AtomicBoolean running = new AtomicBoolean(false);
	
		public MasterListener() {
		}
	
		public MasterListener(List<String> masters, String host, int port) {
		    this.masters = masters;
		    this.host = host;
		    this.port = port;
		}
	
		public MasterListener(List<String> masters, String host, int port,
			long subscribeRetryWaitTimeMillis) {
		    this(masters, host, port);
		    this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
		}
	
		public void run() {
	
		    running.set(true);
	
		    while (running.get()) {
	
			jedis = new Jedis(host, port);
	
			try {
				jedis.subscribe(new JedisPubSubAdapter(masters, host, port), "+odown", "-odown", "+switch-master");
	
			} catch (JedisConnectionException e) {
	
			    if (running.get()) {
					LOGGER.error("Lost connection to Sentinel at " + host
						+ ":" + port
						+ ". Sleeping 5000ms and retrying.");
					try {
					    Thread.sleep(subscribeRetryWaitTimeMillis);
					} catch (InterruptedException e1) {
					    e1.printStackTrace();
					}
			    } else {
					LOGGER.info("Unsubscribing from Sentinel at " + host + ":"
						+ port);
			    }
			}
		    }
		}
	
		public void shutdown() {
		    try {
				LOGGER.info("Shutting down listener on " + host + ":" + port);
				running.set(false);
				// This isn't good, the Jedis object is not thread safe
				jedis.disconnect();
		    } catch (Exception e) {
		    	LOGGER.error("Caught exception while shutting down: " + e.getMessage());
		    }
		}
    }
}