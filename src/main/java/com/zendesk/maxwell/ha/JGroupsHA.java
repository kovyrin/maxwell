package com.zendesk.maxwell.ha;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.RaftHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellHA;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A jgroups-raft-based implementation of high-availability for maxwell.
 */
public class JGroupsHA extends MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(JGroupsHA.class);

	private final String jgroupsConf, raftMemberID;
	private AtomicBoolean isRaftLeader = new AtomicBoolean(false);

	/**
	 * Build a MaxwellHA object
	 * @param maxwell The Maxwell instance that will be run when an election is won
	 * @param clientID The maxwell clientID.  Used to create a unique "channel" for the election
	 * @param jgroupsConf Path to an xml file that will configure the RAFT cluster
	 * @param raftMemberID unique ID identifying the raft member in the cluster
	 */
	public JGroupsHA(Maxwell maxwell, String clientID, String jgroupsConf, String raftMemberID) {
		super(maxwell, clientID);

		this.jgroupsConf = jgroupsConf;
		this.raftMemberID = raftMemberID;
	}

	/**
	 * Join the raft cluster, starting and stopping Maxwell on elections.
	 *
	 * Does not return.
	 * @throws Exception if there's any issues
	 */
	@Override
	public void startHA() throws Exception {
		JChannel ch=new JChannel(jgroupsConf);
		RaftHandle handle=new RaftHandle(ch, null);
		if ( raftMemberID != null )
			handle.raftId(raftMemberID);
		else
			LOGGER.warn("--raft_member_id not specified, using values from " + jgroupsConf);

		handle.addRoleListener(role -> {
			if(role == Role.Leader) {
				LOGGER.info("won HA election, starting maxwell");
				isRaftLeader.set(true);
				run();
				isRaftLeader.set(false);
			} else if ( this.isRaftLeader.get() ) {
				LOGGER.info("Unable to find consensus, stepping down HA leadership");
				stop();
				isRaftLeader.set(false);
			} else {
				LOGGER.info("lost HA election, current leader: " + handle.leader());
			}
		});

		ch.connect(this.clientID);
		LOGGER.info("enter HA group, current leader: " +  handle.leader());

		Thread.sleep(Long.MAX_VALUE);
	}
}
