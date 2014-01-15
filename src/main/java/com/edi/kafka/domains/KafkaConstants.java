package com.edi.kafka.domains;

/**
 * Constants of all
 * @author Edison Xu
 *
 * Jan 15, 2014
 */
public interface KafkaConstants {

	int DEFAULT_REFRESH_FRE_SEC = 60;
	int INIT_TIMEOUT_MIN = 2; // 2min
	String BROKER_LIST = "metadata.broker.list";
	String PRODUCER_TYPE = "producer.type";
}
