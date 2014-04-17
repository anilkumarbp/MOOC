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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementQueue;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.Management;

/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();

	public AtomicInteger yesCount = new AtomicInteger(0);
	public AtomicInteger noCount = new AtomicInteger(0);
	//depends upon number of nodes in cluster
	public AtomicInteger total = new AtomicInteger(0); 
	
	private String nodeId;
	private ServerConf conf;
	
	public List<InetSocketAddress> addressList = new CopyOnWriteArrayList<InetSocketAddress>();

	public static JobManager getInstance(String id, ServerConf conf) {
		instance.compareAndSet(null, new JobManager(id,conf));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId,ServerConf conf) {
		this.nodeId = nodeId;
		this.conf = conf;
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req, Channel channel, SocketAddress sa) {
		
		//If Current Leader receives request, he forwards it to all slaves
		if(ElectionManager.getInstance().isLeader())
		{
		for (NodeDesc nn : conf.getRoutingList()) {
			InetSocketAddress isa = new InetSocketAddress( nn.getHost(), nn.getMgmtPort());
			if((Integer.parseInt(nn.getNodeId())!=Integer.parseInt(nodeId)))
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
				cf.channel().writeAndFlush(req);
				logger.info("PAXOS request forwarded to slaves by leader" + nodeId);
				}
				cf.channel().closeFuture();
				}
														
				catch(Exception e){
					
					logger.info("PAXOS request refused by " + isa.getHostName() + ":" +isa.getPort()); 
						}
			}
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			}
		}
		
		else
		{
			
			Random random = new Random();
			int bid = random.nextInt(2);
			JobBid.Builder jbid = JobBid.newBuilder();
			jbid.setJobId(req.getJobId());
			jbid.setOwnerId(req.getOwnerId());
			jbid.setNameSpace(req.getNameSpace());
			jbid.setBid(bid);
			
			Management.Builder msg = Management.newBuilder();
			msg.setJobBid(jbid.build());
			ManagementQueue.enqueueResponse(msg.build(), channel, sa);
			
			
		}
			
	}

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req, Channel channel, SocketAddress sa) {
		
	
	long own = req.getOwnerId();	
	//This needs to be updated to include Cluster Leader ID as Originator	
	if(req.getOwnerId()==Long.parseLong(ElectionManager.getInstance().getLeaderId()))
	{	
		
	}
	else
	{	
	int globalBid = 0;	
	if (req.getBid()==1)  yesCount.getAndIncrement();  
	else noCount.getAndIncrement();
	
	total.getAndIncrement();
	
	//total depends upon number of nodes in cluster
	if(total.get()==5 && yesCount.get()>total.get()/2)  globalBid = 1;
	
	JobBid.Builder jbid = JobBid.newBuilder();
	jbid.setJobId(req.getJobId());
	jbid.setOwnerId(req.getOwnerId());
	jbid.setNameSpace(req.getNameSpace());
	jbid.setBid(globalBid);
	
	Management.Builder msg = Management.newBuilder();
	msg.setJobBid(jbid.build());
	ManagementQueue.enqueueResponse(msg.build(), channel, sa);
	
	}
		
	}
}
