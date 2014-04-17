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
package poke.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.comm.CommConnection;
import poke.client.comm.CommListener;
import eye.Comm.Course;
import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.NameSpaceOperation;
import eye.Comm.NameSpaceOperation.SpaceAction;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.Request;
import eye.Comm.User;

/**
 * The command class is the concrete implementation of the functionality of our
 * network. One can view this as a interface or facade that has a one-to-one
 * implementation of the application to the underlining communication.
 * 
 * IN OTHER WORDS (pay attention): One method per functional behavior!
 * 
 * @author gash
 * 
 */
public class ClientCommand {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String host;
	private int port;
	private CommConnection comm;

	public ClientCommand(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	private void init() {
		comm = new CommConnection(host, port);
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(CommListener listener) {
		comm.addListener(listener);
	}

	/**
	 * Our network's equivalent to ping
	 * 
	 * @param tag
	 * @param num
	 */
	public void poke(String tag, int num) {
		// data to send
		/*Ping.Builder f = eye.Comm.Ping.newBuilder();
		f.setTag(tag);
		f.setNumber(num);*/
		
		/*User.Builder f = User.newBuilder();
		f.setUserId("MOOC-7");
		f.setUserName("akwattal");
		f.setPassword("123");*/
		
		Course.Builder f = Course.newBuilder();
		f.setCourseId("C-13");
		f.setCourseName("Machine Learning-2");
		f.setCourseDescription("This is a course offered for Stanford");
		
		NameSpaceOperation.Builder b = NameSpaceOperation.newBuilder();
		b.setAction(SpaceAction.LISTSPACES);
		b.setCId(f.build());
		//b.setUId(f.build());
		
		// payload containing data
		/*Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setPing(f.build());
		r.setBody(p.build());*/
		
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setSpaceOp(b.build());
		r.setBody(p.build());
		
		// header with routing info
		/*eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.PING);
		r.setHeader(h.build());*/
		
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setRoutingId(eye.Comm.Header.Routing.NAMESPACES);
		r.setHeader(h.build());
		
		
		eye.Comm.Request req = r.build();

		try {
			comm.sendMessage(req);
		} catch (Exception e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

}
