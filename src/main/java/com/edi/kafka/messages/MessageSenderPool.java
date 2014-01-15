package com.edi.kafka.messages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edi.kafka.domains.KafkaConstants;
import com.edi.kafka.domains.ZkHosts;
import com.edi.kafka.util.DynamicBrokersReader;

/**
 * A pool to manage the message sender instances.
 * @author Edison Xu
 *
 * Dec 23, 2013
 */
public class MessageSenderPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageSenderPool.class);
	
	private final Semaphore freeSender;
	private final LinkedBlockingQueue<MessageSender> queue;
	private final String topic;
	private final String brokerStr;
	private final int poolSize;
	private final ExecutorService pool;
	private final Properties props;
	private ReadWriteLock closingLock;
	
	/**
	 * A pool to manage the sender instances.
	 * @param size the pool size
	 * @param topic the topic of Kafka
	 * @param brokerStr broker list in String
	 */
	public MessageSenderPool(int size, String topic, String brokerStr)
	{
		this(size,topic,brokerStr,null);
	}
	
	/**
	 * 
	 * A pool to manage the sender instances.
	 * @param size the pool size
	 * @param topic the topic of Kafka
	 * @param brokerStr broker list in String
	 * @param props the config properties
	 */
	public MessageSenderPool(int size, String topic, String brokerStr, Properties props)
	{
		closingLock = new ReentrantReadWriteLock();
		poolSize = size;
		this.freeSender = new Semaphore(size);
		this.queue = new LinkedBlockingQueue<>(size);
		this.topic = topic;
		this.brokerStr = brokerStr;
		pool = Executors.newFixedThreadPool(poolSize);
		this.props = new Properties(props);
		this.props.put(KafkaConstants.BROKER_LIST, brokerStr);
		init();
	}
	
	/**
	 * 
	 * A pool to manage the sender instances.
	 * @param size the pool size
	 * @param topic the topic of Kafka
	 * @param zkhosts the zookeeper hosts of Kafka
	 */
	public MessageSenderPool(int size, String topic, ZkHosts zkhosts)
	{
		this(size,topic,zkhosts,null);
	}
	
	/**
	 * 
	 *  A pool to manage the sender instances.
	 * @param size the pool size
	 * @param topic the topic of Kafka
	 * @param zkhosts the zookeeper hosts of Kafka
	 * @param props the config properties
	 */
	public MessageSenderPool(int size, String topic, ZkHosts zkhosts, Properties props)
	{
		closingLock = new ReentrantReadWriteLock();
		poolSize = size;
		this.freeSender = new Semaphore(size);
		this.queue = new LinkedBlockingQueue<>(size);
		this.topic = topic;
		pool = Executors.newFixedThreadPool(poolSize);
		
		@SuppressWarnings("rawtypes")
		Map conf = new HashMap();
		DynamicBrokersReader reader = 
				new DynamicBrokersReader(conf, zkhosts.getBrokerZkStr(), zkhosts.getBrokerZkPath(), topic);
		this.brokerStr = reader.getBrokerInfo();
		this.props = new Properties();
		this.props.put(KafkaConstants.BROKER_LIST, this.brokerStr);
		init();
	}
	
	private void init() 
	{
		List<Callable<Boolean>> taskList = new ArrayList<>();
		final CountDownLatch count = new CountDownLatch(poolSize);
		for(int i=0;i<poolSize;i++)
		{
			taskList.add(new InitTask(count, this));
		}
			
		try {
			pool.invokeAll(taskList);
			count.await(KafkaConstants.INIT_TIMEOUT_MIN, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("Failed to init the MessageSenderPool", e);
		}
	}
	
	class InitTask implements Callable<Boolean>
	{
		CountDownLatch count;
		MessageSenderPool pool;
		
		public InitTask(CountDownLatch count, MessageSenderPool pool) {
			this.count = count;
			this.pool = pool;
		}
		@Override
		public Boolean call() throws Exception {
			MessageSenderImpl sender = new MessageSenderImpl(props, topic, pool);
			if(sender!=null)
			{
				queue.offer(sender);
				count.countDown();
			}
			return true;
		}
	}

	public String getBrokerStr() {
		return brokerStr;
	}
	
	/**
	 * Get a sender from the pool within the given timeout
	 * @param waitTimeMillis
	 * how long should it wait for getting the sender instance
	 * @return
	 * a sender instance
	 */
	public MessageSender getSender(long waitTimeMillis) {
		try {
            if (!freeSender.tryAcquire(waitTimeMillis, TimeUnit.MILLISECONDS))
                throw new RuntimeException("Timeout waiting for idle object in the pool.");

        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting for idle object in the pool .");
        } 
		closingLock.readLock().lock();
		MessageSender sender = queue.poll();
		if(sender==null)
		{
			sender = new MessageSenderImpl(props, topic, this);
			if(sender!=null)
			{
				LOGGER.info("Add new sender to the pool.");
				queue.offer(sender);
			}
		}
		closingLock.readLock().unlock();
		return sender;
	}
	
	/**
	 * Return a sender back to pool.
	 */
	public void returnSender(MessageSender sender) {
		if(this.queue.contains(sender))
			return;
		this.queue.offer(sender);
		this.freeSender.release();
	}
	
	public synchronized void close() {
		// lock the thread for closing
		closingLock.writeLock().lock();
		List<Callable<Boolean>> taskList = new ArrayList<>();
		int size = queue.size();
		final CountDownLatch count = new CountDownLatch(queue.size());
		for(int i=0;i<size;i++)
		{
			taskList.add(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					MessageSender sender = queue.poll();
					if(sender!=null)
					{
						sender.shutDown();
						count.countDown();
					}
					return true;
				}
			});
		}
			
		try {
			pool.invokeAll(taskList);
			count.await(KafkaConstants.INIT_TIMEOUT_MIN, TimeUnit.MINUTES);
			pool.shutdownNow();
		} catch (InterruptedException e) {
			LOGGER.error("Failed to close the MessageSenderPool", e);
		}
		closingLock.writeLock().unlock();
	}
}
