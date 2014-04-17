/*
 * copyright 2014, gash
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
package poke.server.management.managers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementQueue;
import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Network.NetworkAction;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();
	public AtomicInteger nominations = new AtomicInteger(0);
	public List<InetSocketAddress> addressList = new CopyOnWriteArrayList<InetSocketAddress>();
		
	private String nodeId, leaderId;
	private boolean isLeader = false;
	ServerConf conf; 
	List<String> nodeList;
	int i;

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	public static ElectionManager getInstance(String id, ServerConf conf, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, conf, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, ServerConf conf, int votes) {
		this.nodeId = nodeId;
		
		this.conf = conf;
		
		if (votes >= 0)
			this.votes = votes;
				
		this.leaderId = conf.getServer().getProperty("leader.id");
			
		this.nodeList = new ArrayList<String>();
	}

	public boolean isLeader() {
		return isLeader;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

	/**
	 * @param args
	 */
	public void processRequest(LeaderElection req, Channel channel, SocketAddress sa) throws NumberFormatException, InterruptedException {
		if (req == null)
			return;

		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			logger.info("Election Started!");
	
			nominateSelf();
			
			} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode`
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
	
			logger.info("Server " +req.getBallotId() + "  was declared as the Leader in the network");
			
			//if(Integer.parseInt(leaderId)>= Integer.parseInt(req.getBallotId()))	
			leaderId = req.getBallotId();	
			if(leaderId.equals(nodeId)) setLeader(true);  //here we need to update leader info with DNS server
			else setLeader(false);
			
		
			
			
				/*
				for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
				if(nn.getNodeId().equals(leaderId))
				{
				HeartbeatData node = new HeartbeatData(nn.getNodeId(), nn.getHost(), nn.getPort(), nn.getMgmtPort());
				HeartbeatConnector.getInstance().addConnectToThisNode(node);
				}
			
			} */		
			
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			//logger.info("Nomination rec!");
			//nominations.incrementAndGet();
			
			int comparedToMe = req.getBallotId().compareTo(nodeId);
			if (comparedToMe == -1) {
				// Someone else has a higher priority, forward nomination
				// TODO forward
				leaderId = req.getBallotId();
			} else if (comparedToMe == 1) {
				// I have a higher priority, nominate myself
				// TODO nominate myself
				nominateSelf();
				
			
			}
			else if(req.getBallotId().equals(nodeId)) declareSelfWinner();
		}
	}

	public String getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(String leaderId) {
		this.leaderId = leaderId;
	}
	
	/*
	public void nominateSelf() throws InterruptedException
	{
		//int rounds = Integer.parseInt(conf.getServer().getProperty("diameter"));
		//for(int i=0; i<rounds;i++)
		//{
			LeaderElection.Builder le = LeaderElection.newBuilder();
			le.setNodeId(nodeId);
			le.setBallotId(nodeId);
			//System.out.println(leaderId);
			le.setVote(VoteAction.NOMINATE);
			le.setDesc(nodeId);
			Management.Builder msg = Management.newBuilder();
			msg.setElection(le.build());
			
			for (NodeDesc nn : conf.getRoutingList()) {
				try
				{ InetSocketAddress isa = new InetSocketAddress( nn.getHost(), nn.getMgmtPort());
				ChannelFuture cf = ManagementQueue.connect(isa);
				cf.awaitUninterruptibly(50001);
				if(cf.isDone()&&cf.isSuccess())
				{
				cf.channel().writeAndFlush(msg.build());
				logger.info("Nomination sent by " + nodeId);
				}
				cf.channel().closeFuture();
				
				}
														
				catch(Exception e){logger.info("Election Message refused by " + nn.getHost() + ":" +nn.getMgmtPort());}
			}
				Thread.sleep(10000);
				//if(leaderId==nodeId) declareSelfWinner();
				if(Integer.parseInt(getLeaderId())>Integer.parseInt(nodeId))
					nominateSelf();
					else if(Integer.parseInt(getLeaderId())==Integer.parseInt(nodeId))
							declareSelfWinner();
							
		}
	//}
	 * 
	 * 
	 */
	
	public void declareSelfWinner()
	{
		setLeader(true);
		//int rounds = Integer.parseInt(conf.getServer().getProperty("diameter"));
		//for(int i=0; i<rounds;i++)
		//	{
			LeaderElection.Builder le = LeaderElection.newBuilder();
			le.setNodeId(nodeId);
			le.setBallotId(nodeId);
			//System.out.println(leaderId);
			le.setVote(VoteAction.DECLAREWINNER);
			le.setDesc(nodeId);
			Management.Builder msg = Management.newBuilder();
			msg.setElection(le.build());
			
			for (NodeDesc nn : conf.getRoutingList()) {
				try
				{ InetSocketAddress isa = new InetSocketAddress( nn.getHost(), nn.getMgmtPort());
				ChannelFuture cf = ManagementQueue.connect(isa);
				cf.awaitUninterruptibly(50001);
				if(cf.isDone()&&cf.isSuccess())
				{
				cf.channel().writeAndFlush(msg.build());
				logger.info("Winner sent by " + nodeId);
				}
				cf.channel().closeFuture();
				}
														
				catch(Exception e){logger.info("Election Message refused by " + nn.getHost() + ":" +nn.getMgmtPort());}
			}
			
									
		// }
			
					
	}
	
	public void nominateSelf() throws InterruptedException{
		
		LeaderElection.Builder le = LeaderElection.newBuilder();
		le.setNodeId(nodeId);
		le.setBallotId(nodeId);
		//System.out.println(leaderId);
		le.setVote(VoteAction.NOMINATE);
		le.setDesc(nodeId);
		Management.Builder msg = Management.newBuilder();
		msg.setElection(le.build());
		addressList.clear();
		
		
		for (NodeDesc nn : conf.getRoutingList()) {
			InetSocketAddress isa = new InetSocketAddress( nn.getHost(), nn.getMgmtPort());
			if((Integer.parseInt(nn.getNodeId())<Integer.parseInt(nodeId)))
			addressList.add(isa);
			}

			if(addressList.size()>0)
			{
			for(InetSocketAddress isa : addressList)	{
				try
				{ 
				ChannelFuture cf = ManagementQueue.connect(isa);
				cf.awaitUninterruptibly(50001);
				if(cf.isDone()&&cf.isSuccess())
				{
				cf.channel().writeAndFlush(msg.build());
				logger.info("Nomination sent by " + nodeId);
				}
				cf.channel().closeFuture();
				}
														
				catch(Exception e){
					
					logger.info("Election Message refused by " + isa.getHostName() + ":" +isa.getPort()); 
					addressList.remove(isa); 
					if(addressList.size()>0) continue;
					else declareSelfWinner();
						}
			}
			
			Thread.sleep(10000);
			if(Integer.parseInt(getLeaderId())>Integer.parseInt(nodeId))
				nominateSelf();
				
			}	
			else declareSelfWinner();
				
			
	}
	
	}
