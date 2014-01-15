package com.edi.kafka.messages;

/**
 * The decoder to decode the message to be sent.
 * 
 * @author Edison Xu
 *
 * Dec 23, 2013
 */
public interface MessageDecoder<T> {

	byte[] decode(T msg);
}
