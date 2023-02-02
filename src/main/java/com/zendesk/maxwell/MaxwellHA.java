package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHA.class);

	protected final Maxwell maxwell;
	protected final String clientID;
	private boolean hasRun = false;

	/**
	 * Build a MaxwellHA object
	 * @param maxwell The Maxwell instance that will be run when an election is won
	 * @param clientID The maxwell clientID.  Used to create a unique "channel" for the election
	 */
	public MaxwellHA(Maxwell maxwell, String clientID) {
		this.maxwell = maxwell;
		this.clientID = clientID;
	}

	protected void run() {
		try {
			if (hasRun)
				maxwell.restart();
			else
				maxwell.start();

			hasRun = true;
		} catch ( Exception e ) {
			LOGGER.error("Maxwell terminating due to exception:", e);
			System.exit(1);
		}
	}

	protected void stop() {
		maxwell.terminate();
	}

	/**
	 * Override with a specific HA implementation that calls run() when elected as the leader
	 * and calls stop() when the leadership is lost.
	 */
	abstract public void startHA() throws Exception;
}
