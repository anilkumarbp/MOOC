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
package poke.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import poke.domain.Course;
import poke.domain.User;
import poke.server.resources.Resource;
import poke.server.storage.MongoDBDAO;
import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.NameSpace;
import eye.Comm.NameSpaceOperation;
import eye.Comm.NameSpaceStatus;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.NameSpaceOperation.SpaceAction;
import eye.Comm.Request.Builder;

public class NameSpaceResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	@Override
	public Request process(Request request) throws FileNotFoundException, IOException {
		
		// TODO Auto-generated method stub
		Request reply = buildMessage(request,PokeStatus.NOFOUND, "Request not fulfilled", request.getBody().getSpaceOp().getAction());
		
		MongoDBDAO mclient = new MongoDBDAO();
		try {
			
			mclient.getDBConnection();
			mclient.getDB(mclient.getDbName());
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
		
		//If Request is for User CRUD operations
		if(request.getBody().getSpaceOp().hasUId())	{
			
			mclient.getCollection("usercollection");
			User user = new User();
			user.setUserId(request.getBody().getSpaceOp().getUId().getUserId());
			user.setName(request.getBody().getSpaceOp().getUId().getUserName());
			user.setPassword(request.getBody().getSpaceOp().getUId().getPassword());
			user.setCity(request.getBody().getSpaceOp().getUId().getCity());
			user.setZipCode(request.getBody().getSpaceOp().getUId().getZipcode());
			
			switch(request.getBody().getSpaceOp().getAction())	
			{
			case ADDSPACE:
				BasicDBObject doc = new BasicDBObject("userid",user.getUserId()).append("username", user.getName()).append("password", user.getPassword()).append("city", user.getCity()).append("zipcode", user.getZipCode());
				mclient.insertData(doc);
				reply = buildMessage(request,PokeStatus.SUCCESS, "User added to database",SpaceAction.ADDSPACE);
				break;
			
			case LISTSPACES:
				int authenticated=400;
				BasicDBObject query1 = new BasicDBObject();
				List<BasicDBObject> query1List = new ArrayList<BasicDBObject>();
				query1List.add(new BasicDBObject("username", user.getName()));
				query1List.add(new BasicDBObject("password", user.getPassword()));
				query1.put("$and", query1List);

				DBCursor cursor = mclient.findData(query1);
				while (cursor.hasNext()) {
					cursor.next();
					authenticated=200;
				}
				
				if(authenticated==200)
					reply = buildMessage(request,PokeStatus.SUCCESS, "User login successful", SpaceAction.LISTSPACES);
				else	
					reply = buildMessage(request,PokeStatus.FAILURE, "User login failed!", SpaceAction.LISTSPACES);	
				break;
				
			case REMOVESPACE:
				BasicDBObject rem = new BasicDBObject("userid",user.getUserId());
				mclient.deleteData(rem);
				reply = buildMessage(request,PokeStatus.SUCCESS, "User deleted", SpaceAction.REMOVESPACE);
				break;
				
			case UPDATESPACE:
				BasicDBObject que = new BasicDBObject("userid",user.getUserId());
				BasicDBObject upd = new BasicDBObject("userid",user.getUserId()).append("username", user.getName()).append("password", user.getPassword()).append("city", user.getCity()).append("zipcode", user.getZipCode());
				mclient.updateData(que, upd);
				reply = buildMessage(request,PokeStatus.SUCCESS, "User updated", SpaceAction.UPDATESPACE);
				break;
				
			default:
				break;
				}
			}
		
		//If Request is for Course CRUD operations
		else if(request.getBody().getSpaceOp().hasCId()) {
			mclient.getCollection("coursecollection");
			Course course = new Course();
			course.setCourseId(request.getBody().getSpaceOp().getCId().getCourseId());
			course.setCourseName(request.getBody().getSpaceOp().getCId().getCourseName());
			course.setCourseDescription(request.getBody().getSpaceOp().getCId().getCourseDescription());
			course.setAddCode(request.getBody().getSpaceOp().getCId().getAddCode());
					
			switch(request.getBody().getSpaceOp().getAction()) {
			case ADDSPACE:
				BasicDBObject doc = new BasicDBObject("courseid",course.getCourseId()).append("coursename", course.getCourseName()).append("coursedesc", course.getCourseDescription()).append("addcode", course.getAddCode());
				mclient.insertData(doc);
				reply = buildMessage(request,PokeStatus.SUCCESS, "Course added to database", SpaceAction.ADDSPACE);				
				break;
				
			case LISTSPACES:	
				BasicDBObject view = new BasicDBObject("courseid",course.getCourseId());
				DBCursor cursor = mclient.findData(view);
				Request.Builder r = Request.newBuilder();
				eye.Comm.Course.Builder f = eye.Comm.Course.newBuilder();
				
				while (cursor.hasNext()) {
					f.setCourseId((String) cursor.next().get("courseid"));
					f.setCourseName((String) cursor.curr().get("coursename"));
					f.setCourseDescription((String) cursor.curr().get("coursedesc"));
					}
				
				NameSpaceOperation.Builder b = NameSpaceOperation.newBuilder();
				b.setAction(SpaceAction.LISTSPACES);
				b.setCId(f.build());
				
				eye.Comm.Payload.Builder p = Payload.newBuilder();
				p.setSpaceOp(b.build());
				r.setBody(p.build());
				
				eye.Comm.Header.Builder h = Header.newBuilder();
				h.setOriginator("client");
				h.setRoutingId(eye.Comm.Header.Routing.NAMESPACES);
				h.setReplyMsg("Course Details");
				r.setHeader(h.build());
				
				reply = r.build();
				break;
				
			case REMOVESPACE:
				BasicDBObject rem = new BasicDBObject("courseid",course.getCourseId());
				mclient.deleteData(rem);
				reply = buildMessage(request,PokeStatus.SUCCESS, "Course deleted", SpaceAction.REMOVESPACE);
				break;
				
			case UPDATESPACE:
				BasicDBObject que = new BasicDBObject("courseid",course.getCourseId());
				BasicDBObject upd =  new BasicDBObject("courseid",course.getCourseId()).append("coursename", course.getCourseName()).append("coursedesc", course.getCourseDescription()).append("addcode", course.getAddCode());
				mclient.updateData(que, upd);
				reply = buildMessage(request,PokeStatus.SUCCESS, "Course updated", SpaceAction.UPDATESPACE);
				break;
				
			default:
				break;}
			}
		
		mclient.closeConnection();
		return reply;
		}
	
	public Request buildMessage(Request request,PokeStatus pks, String message, SpaceAction spAction)
	{
	/*NameSpaceStatus.Builder ns = NameSpaceStatus.newBuilder();
	ns.setStatus(pks);
	Payload.Builder py = Payload.newBuilder();
	py.setSpaceStatus(ns.build());
	Header.Builder he = Header.newBuilder();
	he.setRoutingId(Routing.NAMESPACES);
	he.setOriginator(request.getHeader().getOriginator());
	he.setReplyMsg(message);
	he.setReplyCode(pks);
	reply.setHeader(he.build());
	reply.setBody(py.build());*/
		
	//If CRUD is for User
	if (request.getBody().getSpaceOp().hasUId()) {
		Request.Builder r = Request.newBuilder();
		eye.Comm.User.Builder f = eye.Comm.User.newBuilder();
		f.setUserId(request.getBody().getSpaceOp().getUId().getUserId());
		
		NameSpaceOperation.Builder b = NameSpaceOperation.newBuilder();
		b.setAction(spAction);
		b.setUId(f.build());
		
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setSpaceOp(b.build());
		r.setBody(p.build());
		
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setRoutingId(eye.Comm.Header.Routing.NAMESPACES);
		h.setReplyMsg(message);
		r.setHeader(h.build());
		
		eye.Comm.Request reply = r.build();
		
		return reply;
		}
	
	//If CRUD is for Course
	else if(request.getBody().getSpaceOp().hasCId()) {
		Request.Builder r = Request.newBuilder();
		eye.Comm.Course.Builder f = eye.Comm.Course.newBuilder();
		f.setCourseId(request.getBody().getSpaceOp().getCId().getCourseId());
		
		NameSpaceOperation.Builder b = NameSpaceOperation.newBuilder();
		b.setAction(spAction);
		b.setCId(f.build());
		
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setSpaceOp(b.build());
		r.setBody(p.build());
		
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setRoutingId(eye.Comm.Header.Routing.NAMESPACES);
		h.setReplyMsg(message);
		r.setHeader(h.build());
		
		eye.Comm.Request reply = r.build();
		
		return reply;
		} 
	
		// payload containing data
		/*Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setPing(f.build());
		r.setBody(p.build());*/
		
		//Request.Builder r = Request.newBuilder();
		
		
		// header with routing info
		/*eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.PING);
		r.setHeader(h.build());*/
		
		/*eye.Comm.Course.Builder f = eye.Comm.Course.newBuilder();
		f.setCourseId("C-12");
		f.setCourseName("Machine Learning");
		f.setCourseDescription("This is a course offered for Stanford");
		
		NameSpaceOperation.Builder b = NameSpaceOperation.newBuilder();
		b.setAction(SpaceAction.ADDSPACE);
		b.setCId(f.build());*/
	return null;
	}
	}
