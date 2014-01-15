package com.edi.kafka.domains;

/**
 * 
 * @author Edison Xu
 *
 * Dec 20, 2013
 */
public class ZkHosts{

	private String brokerZkStr = null;        
	private String brokerZkPath = null;
	private static String DEFAULT_ZK_ROOT = "/brokers";
	private int refreshFreqSecs = KafkaConstants.DEFAULT_REFRESH_FRE_SEC;
    
	public ZkHosts(String brokerZkStr) {
        this.brokerZkStr = brokerZkStr;
        this.brokerZkPath = DEFAULT_ZK_ROOT;
    }
	
    public ZkHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerZkStr = brokerZkStr;
        this.brokerZkPath = brokerZkPath;
    }

	public String getBrokerZkStr() {
		return brokerZkStr;
	}

	public void setBrokerZkStr(String brokerZkStr) {
		this.brokerZkStr = brokerZkStr;
	}

	public String getBrokerZkPath() {
		return brokerZkPath;
	}

	public void setBrokerZkPath(String brokerZkPath) {
		this.brokerZkPath = brokerZkPath;
	}

	public int getRefreshFreqSecs() {
		return refreshFreqSecs;
	}

	public void setRefreshFreqSecs(int refreshFreqSecs) {
		this.refreshFreqSecs = refreshFreqSecs;
	}
    
}
