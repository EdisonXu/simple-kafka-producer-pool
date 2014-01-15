package com.edi.kafka.messages;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Implementation of MessageSender
 * @author Edison Xu
 *
 * Dec 20, 2013
 */
public class MessageSenderImpl implements MessageSender{

	private Producer<String, byte[]> producer;
	private String topic;
	private MessageSenderPool pool;
	
	public MessageSenderImpl(Properties props, String topic, MessageSenderPool pool) {
		super();
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<String, byte[]>(config);
		this.topic = topic;
		this.pool = pool;
	}

	public void send(byte[] msg) {
		KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, msg);
		this.producer.send(data);
	}
	
	public <T> void send(T msg, MessageDecoder<T> decoder)
	{
		byte[] decoded = decoder.decode(msg);
		KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, decoded);
		this.producer.send(data);
	}

	public void close() {
		this.pool.returnSender(this);
	}
	
	public void shutDown() {
		this.producer.close();
	}
}
