package com.zendesk.maxwell.ha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellHA;

public class ZookeeperHA extends MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperHA.class);

	private final String zookeeperHosts;

	public ZookeeperHA(Maxwell maxwell, String clientID, String zookeeperHosts) {
		super(maxwell, clientID);
		this.zookeeperHosts = zookeeperHosts;
	}

	@Override
	public void startHA() throws Exception {
	}
}
