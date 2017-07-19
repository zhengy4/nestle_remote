package nestle_sandbox.database;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Time;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.AbstractMap;
import java.util.List;
import java.util.Iterator;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


//import javax.json.JsonObject;
import java.security.InvalidParameterException;

import org.json.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;
import org.mule.util.CaseInsensitiveHashMap;

import com.jcraft.jsch.Logger;

import org.mule.streaming.ConsumerIterator;


import org.json.JSONArray;
import org.json.JSONObject;
//import org.json.parser.JSONParser;
//import org.json.parser.ParseException;
import org.json.*;

import nestle_sandbox.database.TransformIncomingData.testing_enum;
import nestle_sandbox.tools.DBConnection;
import net.sf.json.JSONSerializer;

import org.junit.Test;


 
import static org.junit.Assert.*;


public class Transform_contact implements Callable{
	
private static Log logger = LogFactory.getLog(TransformIncomingData.class);
	
	private String dbhost;
	private String authinfo;
	//private String dbuser;
	//private String dbpass;
	private String dbname;
	private String datepath;
	
	private Calendar current_calendar;
	
	public enum testing_enum{A_VALUE(1),B_VALUE(2),MAX_COUNTER_BEFORE_DELETION(3),TIME_STAMP_ROLLBACK_SECS(-3), DESCRIPTION_MAX_LENGTH(3999),
		TESTING_SINGLETON(0), //testing only, do nothing
		EXCEPTION_THROWN(0); //maybe rename to NOT_UPDATING_TIME_STAMP
		private int value;
		
		private testing_enum(int value){
			this.value=value;
		}
		
		public void setValue(int input){
			this.value=input;
		}
		
		public int getValue(){
			return value;
		}
	};
	
public String getDbhost() {
		
		if(dbhost==null){
			throw new InvalidParameterException("dbhost is not set"+testing_enum.A_VALUE.getValue());
		}else{
			return dbhost;
		}
	}



	public void setDbhost(String dbhost) {
		
		if(dbhost==null){
			throw new InvalidParameterException("input dbhost can not be null");
		}else{
		
			this.dbhost = dbhost;
		}
	}



	public String getAuthinfo() {
		if(authinfo==null){
			throw new InvalidParameterException("authinfo is not set");
		}else{
			return authinfo;
		}
	}



	public void setAuthinfo(String authinfo) {
		
		if(authinfo==null){
			throw new InvalidParameterException("input authoinfo can not be null");
		}else{
			this.authinfo = authinfo;
		}
	}

	
	
public String getDbname() {
		
		if(dbname==null){
			throw new InvalidParameterException("dbname is not set");
		}else{
			return dbname;
		}
	}



	public void setDbname(String dbname) {
		
		if(dbname==null){
			throw new InvalidParameterException("input dbname can not be null");
		}else{
			this.dbname = dbname;
		}
	}

	public String getDatepath() {
		if(datepath==null){
			throw new InvalidParameterException("datepath is not set");
		}else{
			return datepath;
		}
	}



	public void setDatepath(String datepath) {
		
		if(datepath==null){
			throw new InvalidParameterException("input datepath can not be null");
		}else{
			this.datepath = datepath;
		}
	}

	
	
	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		
		testing_enum.EXCEPTION_THROWN.setValue(0); 
		
		//System.out.println("########the testing variable is: "+testing_enum.TESTING_SINGLETON.getValue());
		current_calendar=Calendar.getInstance();
		
		Connection dbConnection = null;
		
		ResultSet rs;
		
		
		MuleMessage message = eventContext.getMessage();
		
		String json_payload = (message.getInvocationProperty("incoming_json"));
        //System.out.println("JSON_VALUE"+message.getInvocationProperty("incoming_json"));
        JSONArray jsonArray = new JSONArray(json_payload);
		
        
dbConnection = DBConnection.getConnection(dbhost, authinfo, dbname);
        
        PreparedStatement insertion_prp_statement=null;
        
		Statement statement=dbConnection.createStatement(); //rs(result set) loop only
		
		Statement execution_statement=dbConnection.createStatement();
		
LinkedHashMap<String, Object> tobe_updated=new LinkedHashMap<String, Object>();
		
		JSONObject jsonObject;
		
		for (int i = 0; i < jsonArray.length(); i++) { //TODO: put length outside of loop incase length() changes dynamically
        	
            jsonObject = jsonArray.getJSONObject(i);
            
            String queryString="select * from Contact where Id='"+jsonObject.getString("Id")+"'";
			
            rs=statement.executeQuery(queryString);
            
            Iterator<?> to_be_updated_keys=jsonObject.keys();
			 
			 
			 SimpleDateFormat SDF_SF_side=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				SDF_SF_side.setTimeZone(TimeZone.getTimeZone("GMT"));
				long SF_side_ms = SDF_SF_side.parse(jsonObject.getString("LastModifiedDate")).getTime();
				
				
			 while(to_be_updated_keys.hasNext()){
				 String key=((String)to_be_updated_keys.next());
				 
					 tobe_updated.put(key, jsonObject.optString(key," "));
				 
			 }
			 
			 
			 
			 if(!rs.next()&&((String)(jsonObject.getString("IsDeleted") ) ).equals("false") ){ //query returns nothing, not in local database record,and incoming IsDeleted=false
					
					 
					 String prp_stmt_insertion_query=prepared_stmt_insertion_string_builder(jsonObject,tobe_updated);
					 
					 insertion_prp_statement=dbConnection.prepareStatement(prp_stmt_insertion_query);
					 
					 try{
					 insertion_prp_statement=this.<String>inst_stmt_prp(insertion_prp_statement,tobe_updated);
					 
					 }
					 catch(Exception e){
						 System.out.println("# prepared statement exception");
					 }
					 
							try{
						insertion_prp_statement.executeUpdate();
						}catch (SQLException e){
					
						testing_enum.EXCEPTION_THROWN.setValue(1);
							}
					
					
							}else if(((String)(jsonObject.getString("IsDeleted") ) ).equals("true")){ //DELETION isDeleted=true and existed
						
					String deletion_query="DELETE FROM nestle_Contact_reason WHERE Id='"+
							jsonObject.getString("Id")+"'";
					
						try{
					execution_statement.executeUpdate(deletion_query);
					}catch(SQLException e){
					
						testing_enum.EXCEPTION_THROWN.setValue(1);
						
					}
				}else{ 	
					if(testing_enum.EXCEPTION_THROWN.getValue()==1){
						return null; //break, since this is insertion
					}
					
					
						SimpleDateFormat SDF_local = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");
					SDF_local.setTimeZone(TimeZone.getTimeZone("GMT"));
				
					long local_ms = SDF_local.parse(rs.getString("LastModifiedDate")).getTime();
					
					
					if(SF_side_ms>local_ms){ //update
						
						StringBuilder update_sb=new StringBuilder("update nestle_Contact_reason SET ");
						
						
						 Iterator<?> update_keys=jsonObject.keys();
						 while(update_keys.hasNext()){
							 	 String key=((String)update_keys.next());
							 if(!key.equals("LastModifiedDate")){
								 update_sb.append(key+"='"+(String)tobe_updated.get(key)+"',");
									 
							 }
							 
						 }
						 
						 update_sb.append("LastModifiedDate="+
								"convert(datetimeoffset,"+
								"'"+
								(String)tobe_updated.get("LastModifiedDate")+"'"+
								",127)" //IMPORTANT! salesforce datatype to sql server 'datetime' type must specify type of 127)
								);

						 update_sb.append(
								" WHERE Id='"+
								(String)tobe_updated.get("Id")+"'");
						 
						
						 String update_query=update_sb.toString();
					
					
						try{
							execution_statement.executeUpdate(update_query);
						}catch (SQLException e){
						
						 
				            	testing_enum.EXCEPTION_THROWN.setValue(1);
				            
								}
						
					}
					
				}
			 
		}
		
		if(testing_enum.EXCEPTION_THROWN.getValue()==0){ //0 NOT thrown, thus write date stamp
			write_date_stamp();
		}
		
	return null;
	}
	
	
	private String prepared_stmt_insertion_string_builder(JSONObject jsonObject,LinkedHashMap<String,? extends Object> tobe_updated){
		Iterator<?> insertion_keys=jsonObject.keys();
		StringBuilder insertion_sb_fields=new StringBuilder("insert into nestle_Contact_reason (");
		StringBuilder insertion_sb_values=new StringBuilder(" values(");
		
		 while(insertion_keys.hasNext()){
			 //String key=((String)insertion_keys.next()).replaceAll("Contact_Reason_Global__r.", "");
			 String key=((String)insertion_keys.next());
			 
			 if(insertion_keys.hasNext()){
				 insertion_sb_fields.append(key+",");
				 insertion_sb_values.append("?,");
				// insertion_sb_values.append("\""+(String)tobe_updated.get(key)+"\",");
			 }else{
				 insertion_sb_fields.append(key+")");
				 insertion_sb_values.append("?)");
			 }
			 
		 }
		 
		 
		 
		 return insertion_sb_fields.toString()+insertion_sb_values.toString();
	}
	
	
	
	private <E> PreparedStatement inst_stmt_prp(PreparedStatement prp_stmt,LinkedHashMap<String,? super E> tobe_updated) throws Exception{
		
		int index=1;
		
		for(Map.Entry<String,? super E> entry: tobe_updated.entrySet()){
			
			if(!(entry.getKey().equals("LastModifiedDate"))){
				prp_stmt.setString(index, entry.getValue().toString());
			}else{
				SimpleDateFormat SDF_SF_side=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				SDF_SF_side.setTimeZone(TimeZone.getTimeZone("GMT"));
				
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
				
						prp_stmt.setTimestamp(index, new Timestamp( 
						
						
						SDF_SF_side.parse(
								(String)entry.getValue()
								).getTime()
						
						),
						
						cal
						
						);
			}
			
			
			index++;
		}
		
		return prp_stmt;
		
		
	}
	
	
private void write_date_stamp() throws Exception{
		
		Calendar calendar = current_calendar; // gets a calendar using the default time zone and locale.
		calendar.add(Calendar.SECOND, testing_enum.TIME_STAMP_ROLLBACK_SECS.getValue());
		
		DateFormat date_format_for_store=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		date_format_for_store.setTimeZone(TimeZone.getTimeZone("GMT"));
		
			
		
		 try{
	            //String verify, putData;
	            String filename=this.datepath+"contact_lastmodded_date.txt.txt";
	            File file = new File(filename);
	            
	            
	            file.createNewFile();
	            FileWriter fw = new FileWriter(file);
	            BufferedWriter bw = new BufferedWriter(fw);
	            bw.write(date_format_for_store.format(calendar.getTime()));
	            bw.flush();
	            bw.close();
	            
	            
	           
	        }catch(IOException e){
	        	
	        e.printStackTrace();
	        }
	    }

}
