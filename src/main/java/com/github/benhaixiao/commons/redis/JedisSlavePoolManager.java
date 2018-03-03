package com.github.benhaixiao.commons.redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * 自动获取读库的redis管理器
 * 
 * @author xiaobenhai
 */
public class JedisSlavePoolManager extends AbstractJedisPoolManager {

	private final static Logger logger = LoggerFactory.getLogger(JedisSlavePoolManager.class);

	protected GenericObjectPoolConfig poolConfig;

	protected int timeout = Protocol.DEFAULT_TIMEOUT;

	protected String password;

	protected int database = Protocol.DEFAULT_DATABASE;

	protected String masterName;

	protected LoadBalance loadBalance;

	protected Set<SlaveListener> slaveListeners = new HashSet<SlaveListener>();

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, final GenericObjectPoolConfig poolConfig) {
		this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels) {
		this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
				Protocol.DEFAULT_DATABASE);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, String password) {
		this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, final GenericObjectPoolConfig poolConfig,
			int timeout, final String password) {
		this(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, final GenericObjectPoolConfig poolConfig,
			final int timeout) {
		this(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, final GenericObjectPoolConfig poolConfig,
			final String password) {
		this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
	}

	public JedisSlavePoolManager(String masterName, Set<String> sentinels, final GenericObjectPoolConfig poolConfig,
			int timeout, final String password, final int database) {

		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;
		this.masterName = masterName;

		init(sentinels, masterName);
	}

	protected void init(Set<String> sentinels, String masterName) {
		boolean inited = false;
		for (String sentinel : sentinels) {
			String[] parts = sentinel.split(":");
			String host = parts[0];
			int port = Integer.parseInt(parts[1]);

			if (!inited) {
				Jedis j = new Jedis(parts[0], port);
				List<Map<String, String>> values = j.sentinelSlaves(masterName);
				Set<HostAndPort> servers = toHostAndPorts(values);

				List<String> address = j.sentinelGetMasterAddrByName(masterName);
				HostAndPort masterAddress = new HostAndPort(address.get(0), Integer.parseInt(address.get(1)));

				initLoadBalance(servers, masterAddress);
				j.close();
				inited = true;
			}

			SlaveListener slaveListener = new SlaveListener(masterName, host, port);

			slaveListeners.add(slaveListener);

			slaveListener.start();
		}
	}

	protected synchronized void initLoadBalance(Set<HostAndPort> servers, HostAndPort master) {
		if (loadBalance != null) {
			// 判断当前的服务器列表是否和原来的一致，一致就不创建新的
			List<Node> current = loadBalance.getNodes();
			boolean same = true;
			if (current.size() == servers.size()) {
				for (Node node : current) {
					if (!servers.contains(node.address)) {
						same = false;
						break;
					}
				}
			} else {
				same = false;
			}
			if (same) {
				logger.info("servers not change:" + servers);
				return;
			}
		}

		logger.info("change servers:" + servers + " --> " + "current:"
				+ (loadBalance == null ? null : loadBalance.getNodes()));

		final LoadBalance current = this.loadBalance;

		// 创建新的负载均衡器
		final LoadBalance newLoadBalance = new UsePingBestLoadbalance(servers, master, current);

		this.loadBalance = newLoadBalance;
	}

	public class LoadBalance {

		protected List<Node> nodes;

		protected HostAndPort masterAddress;

		private AtomicInteger seq = new AtomicInteger(0);

		public LoadBalance(Set<HostAndPort> servers, HostAndPort master) {
			this(servers, master, null);
		}

		public LoadBalance(Set<HostAndPort> servers, HostAndPort master, LoadBalance current) {

			this.masterAddress = master;

			List<Node> newNodes = new ArrayList<>(servers.size());
			Set<HostAndPort> newServes = new HashSet<>();
			newServes.addAll(servers);
			if (current != null) {
				List<Node> currnetNodes = current.getNodes();
				for (Node node : currnetNodes) {
					if (servers.contains(node.address)) {
						// 保留有效的节点
						newNodes.add(node);
						newServes.remove(node.address);
					} else {
						// 等待清除
					}
				}
			}
			for (HostAndPort hostAndPort : newServes) {
				// 串讲新节点
				Node node = new Node();
				node.address = hostAndPort;
				node.pool = new JedisPool(poolConfig, hostAndPort.getHost(), hostAndPort.getPort(), timeout, password,
						database);
				newNodes.add(node);
			}

			this.nodes = newNodes;
		}

		public Jedis getJedis() {
			int next = Math.abs(seq.getAndIncrement());

			final List<Node> availables = getAvailableNodes();

			int size = availables.size();

			for (int j = 0; j < size; j++) {
				int p = (next + j) % size;
				JedisPool jedisPool = availables.get(p).pool;
				try {
					Jedis jedis = jedisPool.getResource();
					if (jedis != null) {
						return jedis;
					}
				} catch (JedisConnectionException e) {
					logger.warn("Get jedis from pool({}) failed", p, e);
				}
			}
			throw new JedisException("Cann't get jedis from all pool");
		}

		protected List<Node> getAvailableNodes() {
			return nodes;
		}

		public List<Node> getNodes() {
			return nodes;
		}

	}

	/**
	 * 用ping最低的节点
	 */
	public class UsePingBestLoadbalance extends LoadBalance {

		public final static int DEFAULT_THREDHOLD = 5;

		public final static int PING_TIMES = 3;
		
		public final static boolean PREFER_MASTER = true;

		protected List<Node> availableNodes;

		protected int threshold;
		
		/**
		 * 如果主库的延时更低，会优先读取主库
		 */
		protected boolean preferMaster;

		public UsePingBestLoadbalance(Set<HostAndPort> servers, HostAndPort master) {
			this(servers, master, null, DEFAULT_THREDHOLD);
		}

		public UsePingBestLoadbalance(Set<HostAndPort> servers, HostAndPort master, LoadBalance current) {
			this(servers, master, current, DEFAULT_THREDHOLD);
		}

		public UsePingBestLoadbalance(Set<HostAndPort> servers, HostAndPort master, LoadBalance current,
				int threshold) {
			super(servers, master, current);

			this.threshold = threshold;

			List<Node> nodeList = getNodes();

			// 获取延时最低的节点，并且延时在阈值范围内的加入到可用列表中
			long pingMin = -1;
			for (Node node : nodeList) {
				long pingTime = ping(node);
				if (pingTime == -1) {
					continue;
				}
				if (pingMin == -1 || pingTime < pingMin) {
					pingMin = pingTime;
				}
			}

			availableNodes = new ArrayList<>();

			for (Node node : nodeList) {
				// ping小的放入可用队列
				if (node.ping != -1 && node.ping <= pingMin + threshold) {
					availableNodes.add(node);
				}
			}
			
			if(availableNodes.size() == 0) {
				Node masterNode = new Node();
				masterNode.address = master;
				masterNode.pool = new JedisPool(poolConfig, master.getHost(), master.getPort(), timeout, password,
						database);
				availableNodes.add(masterNode);
			}else if(preferMaster) {
				// 可用列表大于0的情况下，如果主库延时更低，读主库
				Node masterNode = new Node();
				masterNode.address = master;
				masterNode.pool = new JedisPool(poolConfig, master.getHost(), master.getPort(), timeout, password,
						database);
				ping(masterNode);
				if(masterNode.ping < pingMin - threshold) {
					availableNodes.clear();
					availableNodes.add(masterNode);
				}else {
					masterNode.pool.close();
				}
			}

			logger.info("available nodes:" + availableNodes);
		}

		protected long ping(Node node) {
			try (Jedis jedis = node.pool.getResource()) {
				long total = 0;
				for (int i = 0; i < PING_TIMES; i++) {
					long time = System.currentTimeMillis();
					jedis.ping();
					total += System.currentTimeMillis() - time;
				}
				node.ping = total / PING_TIMES;

				return node.ping;
			} catch (Exception e) {
				logger.error("ping " + node + " fail:", e);
				return -1;
			}
		}

		@Override
		protected List<Node> getAvailableNodes() {
			return this.availableNodes;
		}
	}

	public static class Node {
		HostAndPort address;
		JedisPool pool;
		int weight = 0;
		long ping = 0;

		@Override
		public String toString() {
			return address.toString() + "(" + weight + "," + ping + ")";
		}

		@Override
		public boolean equals(Object node2) {
			if (node2 == null || !(node2 instanceof Node)) {
				return false;
			}
			return this.address.equals(node2);
		}

		@Override
		public int hashCode() {
			return address.hashCode();
		}
	}

	protected class SlaveListener extends Thread {

		protected String masterName;
		protected String host;
		protected int port;
		protected long subscribeRetryWaitTimeMillis = 5000;
		protected Jedis j;
		protected AtomicBoolean running = new AtomicBoolean(false);

		public SlaveListener(String masterName, String host, int port) {
			this.masterName = masterName;
			this.host = host;
			this.port = port;
		}

		public SlaveListener(String masterName, String host, int port, long subscribeRetryWaitTimeMillis) {
			this(masterName, host, port);
			this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
		}

		public void run() {

			running.set(true);

			while (running.get()) {

				j = new Jedis(host, port);

				try {
					j.subscribe(new JedisPubSub() {
						@Override
						public void onMessage(String channel, String message) {
							logger.info("Sentinel " + host + ":" + port + " published: " + message + ".");
							// 触发后，重新获取从库列表
							Jedis sentinel = new Jedis(host, port);
							List<Map<String, String>> slaveInfos = sentinel.sentinelSlaves(masterName);
							List<String> address = sentinel.sentinelGetMasterAddrByName(masterName);
							HostAndPort masterAddress = new HostAndPort(address.get(0),
									Integer.parseInt(address.get(1)));
							sentinel.close();
							Set<HostAndPort> hostAndPorts = toHostAndPorts(slaveInfos);
							// 重构负载均衡器
							initLoadBalance(hostAndPorts, masterAddress);
						}
					}, "+switch-master", "+sdown", "-sdown", "+slave");

				} catch (JedisConnectionException e) {

					if (running.get()) {
						logger.warn("Lost connection to Sentinel at " + host + ":" + port
								+ ". Sleeping 5000ms and retrying.");
						try {
							Thread.sleep(subscribeRetryWaitTimeMillis);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					} else {
						logger.info("Unsubscribing from Sentinel at " + host + ":" + port);
					}
				}
			}
		}

		public void shutdown() {
			try {
				logger.info("Shutting down listener on " + host + ":" + port);
				running.set(false);
				// This isn't good, the Jedis object is not thread safe
				j.disconnect();
			} catch (Exception e) {
				logger.error("Caught exception while shutting down: ", e);
			}
		}
	}

	@Override
	public Jedis getJedis() {
		return loadBalance.getJedis();
	}

	protected Set<HostAndPort> toHostAndPorts(List<Map<String, String>> slaveInfos) {
		Set<HostAndPort> result = new HashSet<>(slaveInfos.size());

		for (Map<String, String> slaveInfo : slaveInfos) {

			// 跳过sdown的节点
			String flags = slaveInfo.get("flags");

			if (StringUtils.contains(flags, "disconnected")) {
				logger.info("slave disconnected:" + slaveInfo.get("ip") + ":" + slaveInfo.get("port"));
				continue;
			}

			HostAndPort item = new HostAndPort(slaveInfo.get("ip"), Integer.parseInt(slaveInfo.get("port")));
			result.add(item);
		}
		return result;
	}

}
