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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.HeartMonitor;
import poke.server.management.ManagementQueue.ManagementQueueEntry;

public class OutboundMgmtWorker extends Thread{
	protected static Logger logger = LoggerFactory.getLogger("management");
	
	int workerId;
	boolean forever = true;
	private EventLoopGroup group;

	public OutboundMgmtWorker(ThreadGroup tgrp, int workerId) {
		super(tgrp, "outbound-mgmt-" + workerId);
		this.workerId = workerId;

		if (ManagementQueue.outbound == null)
			throw new RuntimeException("management worker detected null queue");
	}

	@Override
	public void run() {
		while (true) {
			if (!forever && ManagementQueue.outbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				ManagementQueueEntry msg = ManagementQueue.outbound.take();

				if (logger.isDebugEnabled())
					logger.debug("Outbound management message received");
					//logger.info("Outbound management message received");

				if (msg.channel.isWritable()) {
					boolean rtn = false;
					
					if (msg.channel != null && msg.channel.isOpen() && msg.channel.isWritable()) {
						
						SocketAddress socka = msg.sa;
						
						if (socka != null) {
							InetSocketAddress isa = (InetSocketAddress) socka;
							
						//logger.info("Here");
						//System.out.println(msg.req.getElection().getNodeId());
						//if (ManagementQueue.nodeMap.get(msg.req.getElection().getNodeId()) != null) {
						//	InetSocketAddress isa = ManagementQueue.nodeMap.get(msg.req.getElection().getNodeId());
							logger.info("Port" + isa.getPort());
							
							ChannelFuture cf = ManagementQueue.connect(isa);
							cf.awaitUninterruptibly(50001);
							cf.channel().writeAndFlush(msg.req);
							
							rtn = cf.isSuccess();
							if (!rtn)
								ManagementQueue.outbound.putFirst(msg);

					}
					}

				} else
					ManagementQueue.outbound.putFirst(msg);
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("management outbound queue closing");
		}
	
	}
	


	public static class OutboundClosedListener implements ChannelFutureListener {
	private ManagementQueueEntry entry;

	public OutboundClosedListener(ManagementQueueEntry entry) {
		this.entry = entry;
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		logger.info("You made it!");
		//ManagementQueue.outbound.putFirst(entry);
		//entry.channel =  null;
		//entry.sa = null;
		//entry.req = null;
	}
}
	
}
