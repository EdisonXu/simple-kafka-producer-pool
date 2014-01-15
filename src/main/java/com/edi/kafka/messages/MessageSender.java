package com.edi.kafka.messages;

/**
 * 
 * MessageSender interface
 * @author Edison Xu
 *
 * Dec 23, 2013
 */
public interface MessageSender {

	void send(byte[] msg);
	
	/**
	 * Send the msg to Kafka
	 * @param msg
	 * data to be sent
	 * @param encoder
	 * the encoder to encode this msg to byte[]
	 */
	<T> void send(T msg, MessageEncoder<T> encoder);
	
	/**
	 * The sender is not really closed but sent back into pool.
	 */
	void close();
	
	/**
	 * Shutdown this sender, so it could not be used again.
	 */
	public void shutDown();
}
