package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ShardedJedis;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;


public class RedisManager implements Serializable {
	private static final long serialVersionUID = 5751931039068214452L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RedisManager.class);
	
	private String shards = null;
	private String sentinels = null;
	private int database = 0;
	private RedisConnectionPool pool = null;
	private int failCount = 0;
	
	public RedisManager(String shards, String sentinels) {
		this(shards, sentinels, 0);
	}
	
	public RedisManager(String shards, String sentinels, int database) {
		this.shards = shards;
		this.sentinels = sentinels;
		this.database = database;
	}
	
	public void init() {
		this.pool = RedisConnectionPool.getInstance(shards, sentinels, database);
		pool.init();
	}
	
	private void failReport() {
		if (failCount > 10) {
			pool.init();
			failCount = 0;
		}
		failCount++;
	}
	
	public void setPool(RedisConnectionPool pool) {
		this.pool = pool;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(shards);
		out.writeObject(sentinels);
		out.writeInt(database);
	}
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		shards = (String) in.readObject();
		sentinels = (String) in.readObject();
		database = in.readInt();
	}
	
	public ShardedJedis getResource() {
		ShardedJedis resource = null;
		try {
			resource = pool.getResource();
		} catch (Exception e) {
			failReport();
			LOGGER.error("redis failed to get resource", e);
		}
		return resource;
	}
	
	public void returnResource(ShardedJedis jedis) {
		if (jedis != null) {
			pool.returnResource(jedis);
		}
	}
	
	public void returnBrokenResource(ShardedJedis jedis) {
		if (jedis != null) {
			try {
				failReport();
				pool.returnBrokenResource(jedis);
			} catch (Exception e) {
				LOGGER.error("redis failed to return resource", e);
			}
		}
	}
	
	public boolean set(byte[] key, int expire, byte[] value) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.setex(key, expire, value);
			returnResource(jedis);
			return true;
		} catch (Exception e) {
			LOGGER.error("redis set failed", e);
			returnBrokenResource(jedis);
			return false;
		}
	}
	
	public boolean set(String key, int expire, String value) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.setex(key, expire, value);
			returnResource(jedis);
			return true;
		} catch (Exception e) {
			LOGGER.error("redis set failed", e);
			returnBrokenResource(jedis);
			return false;
		}
	}
	
	public boolean hset(String key, int expire, String field, String value) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hset(key, field, value);
			jedis.expire(key, expire);
			returnResource(jedis);
			return true;
		} catch (Exception e) {
			LOGGER.error("redis hset failed", e);
			returnBrokenResource(jedis);
			return false;
		}
	}
	
	public boolean hmset(String key, int expire, Map<String, String> hash) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hmset(key, hash);
			jedis.expire(key, expire);
			returnResource(jedis);
			return true;
		} catch (Exception e) {
			LOGGER.error("redis hmset failed", e);
			returnBrokenResource(jedis);
			return false;
		}
	}
	
	public byte[] get(byte[] key) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			byte[] value = jedis.get(key);
			returnResource(jedis);
			return value;
		} catch (Exception e) {
			LOGGER.error("redis get failed", e);
			returnBrokenResource(jedis);
			return null;
		}
	}
	
	public String get(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			String value = jedis.get(key);
			returnResource(jedis);
			return value;
		} catch (Exception e) {
			LOGGER.error("redis get failed", e);
			returnBrokenResource(jedis);
			return null;
		}
	}
}
