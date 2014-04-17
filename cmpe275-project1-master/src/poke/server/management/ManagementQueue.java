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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Management;

/**
 * The management queue exists as an instance per process (node)
 * 
 * @author gash
 * 
 */
public class ManagementQueue {
	protected static Logger logger = LoggerFactory.getLogger("management");

	protected static LinkedBlockingDeque<ManagementQueueEntry> inbound = new LinkedBlockingDeque<ManagementQueueEntry>();
	protected static LinkedBlockingDeque<ManagementQueueEntry> outbound = new LinkedBlockingDeque<ManagementQueueEntry>();

	// TODO static is problematic
	private static OutboundMgmtWorker oworker;
	private static InboundMgmtWorker iworker;
	private static ChannelFuture channelf;
	static EventLoopGroup group;
	
	public static SortedMap<String, InetSocketAddress> nodeMap;
	public static HashMap<InetSocketAddress, ChannelFuture> channelMap;
	public static ChannelGroup allmgmtChannels;

	// not the best method to ensure uniqueness
	private static ThreadGroup tgroup = new ThreadGroup("ManagementQueue-"
			+ System.nanoTime());

	public static void startup() {
		if (iworker != null)
			return;

		iworker = new InboundMgmtWorker(tgroup, 1);
		iworker.start();
		oworker = new OutboundMgmtWorker(tgroup, 1);
		oworker.start();
		group = new NioEventLoopGroup();
		nodeMap = new TreeMap<String, InetSocketAddress>();
		allmgmtChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
	}

	public static void shutdown(boolean hard) {
		// TODO shutdon workers
	}

	public static void enqueueRequest(Management req, Channel ch,
			SocketAddress sa) {
		try {
			ManagementQueueEntry entry = new ManagementQueueEntry(req, ch, sa);
			inbound.put(entry);
			//InetSocketAddress isa = (InetSocketAddress) sa;
			//System.out.println("Sender Port "+isa.getPort());
			logger.info("Added to Inbound queue");
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public static void enqueueResponse(Management reply, Channel ch, SocketAddress sa) {
		try {
			ManagementQueueEntry entry = new ManagementQueueEntry(reply, ch,
					sa);
			outbound.put(entry);
			//InetSocketAddress isa = (InetSocketAddress) sa;
			//System.out.println("Sender Port "+isa.getPort());
			//logger.info("Added to Outbound queue");
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}
	
	public static ChannelFuture connect(SocketAddress sa)
	{
		
	ManagementInitializer mgtini = new ManagementInitializer(false);	
	Bootstrap b = new Bootstrap();
	b.group(group).channel(NioSocketChannel.class).handler(mgtini);
	b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
	b.option(ChannelOption.TCP_NODELAY, true);
	b.option(ChannelOption.SO_KEEPALIVE, true);
	channelf = b.connect(((InetSocketAddress) sa).getHostName(), ((InetSocketAddress) sa).getPort()).syncUninterruptibly();
	channelf.channel().closeFuture().addListener(new QueueClosedListener());
	return channelf;
	
	}

	public static class ManagementQueueEntry {
		public ManagementQueueEntry(Management req, Channel ch, SocketAddress sa) {
			this.req = req;
			this.channel = ch;
			this.sa = sa;
		}

		public Management req;
		public Channel channel;
		SocketAddress sa;
	}
	

	public static class QueueClosedListener implements ChannelFutureListener {
		
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
				channelf = null;
		}
	}
}
