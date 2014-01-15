package com.edi.kafka.messages;

/**
 * The encoder to encode the message to be sent.
 * 
 * @author Edison Xu
 *
 * Dec 23, 2013
 */
public interface MessageEncoder<T> {

	byte[] encode(T msg);
}
