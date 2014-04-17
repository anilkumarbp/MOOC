/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.management;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.ManagementQueue.ManagementQueueEntry;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.HeartbeatManager;
import poke.server.management.managers.JobManager;
import poke.server.management.managers.NetworkManager;
import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.Network.NetworkAction;

/**
 * The inbound management worker handles the receiving of heartbeats (network
 * status), job bidding, elections.
 * 
 * HB requests to this node are processed here. Nodes making a request to
 * receive heartbeats are in essence requesting to establish an edge (comm)
 * between two nodes. On failure, the connecter must initiate a reconnect - to
 * produce the heartbeatMgr.
 * 
 * On loss of connection: When a connection is lost, the emitter will not try to
 * establish the connection. The edge associated with the lost node is marked
 * failed and all outbound (enqueued) messages are dropped (TBD as we could
 * delay this action to allow the node to detect and re-establish the
 * connection).
 * 
 * Connections are bi-directional (reads and writes) at this time.
 * 
 * @author gash
 * 
 */
public class InboundMgmtWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");

	int workerId;
	boolean forever = true;
	LeaderElection leMessage;
	Network nMessage;
	Channel ch;
	SocketAddress sa;

	public InboundMgmtWorker(ThreadGroup tgrp, int workerId) {
		super(tgrp, "inbound-mgmt-" + workerId);
		this.workerId = workerId;

		if (ManagementQueue.outbound == null)
			throw new RuntimeException("connection worker detected null queue");
	}

	@Override
	public void run() {
		while (true) {
			if (!forever && ManagementQueue.inbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				ManagementQueueEntry msg = ManagementQueue.inbound.take();

				if (logger.isDebugEnabled())
					logger.debug("Inbound management message received");

				Management req = (Management) msg.req;
				if (req.hasBeat()) {
					/**
					 * Incoming: this is from a node that this node requested to
					 * create a connection (edge) to. In other words, we need to
					 * track that this connection is healthy - get a
					 * heartbeatMgr.
					 * 
					 * Incoming are connections this node establishes, which is
					 * handled by the HeartbeatConnector.
					 */
					HeartbeatManager.getInstance().processRequest(req.getBeat());
				} else if (req.hasElection()) {
					logger.info("Election Message received");
					leMessage = req.getElection();
					ch = msg.channel;
					sa = msg.sa;
					//ElectionManager.getInstance().processRequest(req.getElection(), msg.channel, msg.sa);
					new ElectionThread().start();
				} else if (req.hasGraph()) {
					nMessage = req.getGraph();
					ch = msg.channel;
					sa = msg.sa;
					//logger.info("Network Message received");
					//NetworkManager.getInstance().processRequest(req.getGraph(), msg.channel, msg.sa);
					new NetworkThread().start();
				} else if (req.hasJobBid()) {
					logger.info("Job Bid Message received");
					JobManager.getInstance().processRequest(req.getJobBid(), ch, sa);
				} else if (req.hasJobPropose()) {
					logger.info("Job Proposal Message received");
					JobManager.getInstance().processRequest(req.getJobPropose(), ch, sa);
				} else
					logger.error("Unknown management message");

			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected processing failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}
	}
	public class ElectionThread extends Thread{
		public void run(){
							logger.info("Election Message received");
							try {
								ElectionManager.getInstance().processRequest(leMessage, ch, sa);
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
}
	public class NetworkThread extends Thread{
		public void run(){
							logger.info("Network Message received");
							try {
								NetworkManager.getInstance().processRequest(nMessage, ch, sa);
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
}
}