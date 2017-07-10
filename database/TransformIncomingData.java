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

import nestle_sandbox.tools.DBConnection;
import net.sf.json.JSONSerializer;

import org.junit.Test;


 
import static org.junit.Assert.*;


//import com.m1s.filegenerator.GenEnrollmentFile;
//import com.m1s.tool.DBConnection;
//import com.m1s.tool.EncryptCardNo;
//import  com.m1s.tool.Utils;

public class TransformIncomingData implements Callable {
	
	private static Log logger = LogFactory.getLog(TransformIncomingData.class);
	
	private String dbhost;
	private String authinfo;
	//private String dbuser;
	//private String dbpass;
	private String dbname;
	private String datepath;
	
	private String smtp_host;
	private String smtp_port;
	private String smtp_user;
	private String smtp_password;
	
	
	private int counter;
	
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


	public String getSmtp_host() {
		if(smtp_host==null){
			throw new InvalidParameterException("smtp_host is not set");
		}else{
			return smtp_host;
		}
	}



	public void setSmtp_host(String smtp_host) {
		
		if(smtp_host==null){
			throw new InvalidParameterException("input smtp_host can not be null");
		}else{
			this.smtp_host = smtp_host;
		}
	}
	
	public String getSmtp_port() {
		if(smtp_port==null){
			throw new InvalidParameterException("smtp_port is not set");
		}else{
			return smtp_port;
		}
	}



	public void setSmtp_port(String smtp_port) {
		
		if(smtp_port==null){
			throw new InvalidParameterException("input smtp_port can not be null");
		}else{
			this.smtp_port = smtp_port;
		}
	}
	
	public String getSmtp_user() {
		if(smtp_user==null){
			throw new InvalidParameterException("smtp_user is not set");
		}else{
			return smtp_user;
		}
	}



	public void setSmtp_user(String smtp_user) {
		
		if(smtp_user==null){
			throw new InvalidParameterException("input smtp_user can not be null");
		}else{
			this.smtp_user = smtp_user;
		}
	}
	
	public String getSmtp_password() {
		if(smtp_password==null){
			throw new InvalidParameterException("smtp_password is not set");
		}else{
			return smtp_password;
		}
	}



	public void setSmtp_password(String smtp_password) {
		
		if(smtp_password==null){
			throw new InvalidParameterException("input smtp_password can not be null");
		}else{
			this.smtp_password = smtp_password;
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



	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		
		System.out.println("########the testing variable is: "+testing_enum.TESTING_SINGLETON.getValue());
		current_calendar=Calendar.getInstance();
		
		//Thread.sleep(30000);
		
		
		
		Connection dbConnection = null;
		
		ResultSet rs;

		MuleMessage message = eventContext.getMessage();
	
		this.counter++;
		
		logger.info(" The switching bool counter is :"+this.counter +"\n");
		
		logger.info("the smtp_host is "+this.smtp_host+" smtp_port: "+this.smtp_port+" smtp_user: "+this.smtp_user+" smtp_password: "+this.smtp_password);
		
		
		if(this.counter>testing_enum.MAX_COUNTER_BEFORE_DELETION.getValue()){
			message.setInvocationProperty("deletion_check_branch", true); //useless as long as the latter is not needed 
			this.counter=0;
		}else{
			message.setInvocationProperty("deletion_check_branch", false);
		}
		//String jsonValue =message.getInvocationProperty("incoming_json");
		//System.out.println("JSON_VALUE"+jsonValue);
		
		/*
		JSONArray arr=(JSONArray) JSONSerializer.toJSON(message.getInvocationProperty("incoming_json"));
		
		System.out.println("BEFORE LOOPING");
		for(int i=0;i<arr.length();i++){
			JSONObject obj = arr.getJSONObject(i);
			
			JSONArray arr2 = obj.getJSONArray("Id");
			System.out.println("Id is :"+arr2.toString());
		}
		*/
		String json_payload = (message.getInvocationProperty("incoming_json"));
        //System.out.println("JSON_VALUE"+message.getInvocationProperty("incoming_json"));
        JSONArray jsonArray = new JSONArray(json_payload);
        
        JSONArray temp_parsed_Array=new JSONArray();
        
        int old_jsonArray_length=jsonArray.length();
        
        //initial parsing out Contact_Reason_Global__r
        JSONObject json_parsing_Object;
        for (int i = 0; i < old_jsonArray_length; i++) { 
        	
        	
        	json_parsing_Object = jsonArray.getJSONObject(i);
        	Iterator<?> loop_keys=json_parsing_Object.keys();
        	LinkedHashMap<String,String> clear_up_hashmap=new LinkedHashMap<String,String>();
        	
        	//clear_up_hashmap.clone();
        	
        	while(loop_keys.hasNext()){
        		
        		 
        		
				 String key=(String)loop_keys.next();
				 //System.out.println("############The looping key is : "+key+"  the value is: "+json_parsing_Object.optString(key," "));
				 if(key.contains("Contact_Reason_Global__r")){
					 
					 
					 JSONObject contact_reason_j_obj=new JSONObject(json_parsing_Object.optString(key," "));
					 //System.out.println("### Contact_reason is: "+contact_reason_j_obj.toString());
					 Iterator<?> inner_loop_keys=contact_reason_j_obj.keys();
					 while(inner_loop_keys.hasNext()){
						 String inner_key=(String)inner_loop_keys.next();
						 
						 String inner_temp_string=special_char_removal(contact_reason_j_obj.optString(inner_key," "));
						 clear_up_hashmap.put(inner_key,inner_temp_string);
					 }
					 
				 }else if(key.contains("Description")){ //Description might exceeding the database capacity TODO: adding auxillary fields
					 String description_temp=json_parsing_Object.optString(key," ");
					 
					 String outer_temp_string1=special_char_removal(description_temp.substring(0, Math.min(description_temp.length(), testing_enum.DESCRIPTION_MAX_LENGTH.getValue())));
					 clear_up_hashmap.put(key, outer_temp_string1);
				 }else{
					 String outer_temp_string2=special_char_removal(json_parsing_Object.optString(key," "));
					 clear_up_hashmap.put(key, outer_temp_string2);
				 }
				 
			 } //end of while loop
        	
        	temp_parsed_Array.put(new JSONObject(clear_up_hashmap));
        	
        	
        } //end of jsonArray.length() loop
        
        
        System.out.println("############ re-modded json array is : "+temp_parsed_Array.toString());
        
        
        jsonArray=temp_parsed_Array; //force carrying array over
        
        
        
        
        
        
        
        
        
        
        
        
        dbConnection = DBConnection.getConnection(dbhost, authinfo, dbname);
        
        PreparedStatement insertion_prp_statement=null;
        
		Statement statement=dbConnection.createStatement(); //rs(result set) loop only
		
		Statement execution_statement=dbConnection.createStatement();
		/*
		String queryString="select * from nestle_invoice_statement where Id='a0046000002zHNTAA2'";
		
		logger.info(" The query String inside deletion is:"+queryString);
		ResultSet rs=statement.executeQuery(queryString);
		rs.next();
		System.out.println("local modifieddate :"+rs.getString("LastModifiedDate")+"Name :"+rs.getString("Name")+" Id:"+rs.getString("Id"));
		return null;
		
		//testing .....
		*/
		
			
		//AbstractMap<String, Object> tobe_updated=new HashMap<String, Object>(); //disabled for LinkedHashmap
		
		LinkedHashMap<String, Object> tobe_updated=new LinkedHashMap<String, Object>();
		
		JSONObject jsonObject;
		
		//DateTime nestle_dt,local_dt;
		
		//java.sql.Timestamp nestle_dt,local_dt;
		
        for (int i = 0; i < jsonArray.length(); i++) { //TODO: put length outside of loop incase length() changes dynamically
        	
            jsonObject = jsonArray.getJSONObject(i);
            //System.out.println("Id is :" + jsonObject.getString("Id"));
            
            // disabled
            //String queryString="select * from nestle_invoice_statement where Id='"+jsonObject.getString("Id")+"'";
            
            String queryString="select * from nestle_Contact_reason where Id='"+jsonObject.getString("Id")+"'";
			
			//logger.info(" The query String inside deletion is:"+queryString);
            //try{
			 rs=statement.executeQuery(queryString);
           // }catch(SQLException e){
           // 	testing_enum.EXCEPTION_THROWN.setValue(1);
           // }
			
			
			
			//TODO: modify accordingly to data entry
			 
			 Iterator<?> to_be_updated_keys=jsonObject.keys();
			 
			 
			 SimpleDateFormat SDF_SF_side=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				SDF_SF_side.setTimeZone(TimeZone.getTimeZone("GMT"));
				long SF_side_ms = SDF_SF_side.parse(jsonObject.getString("LastModifiedDate")).getTime();
				//long SF_side_ms;
				
			 while(to_be_updated_keys.hasNext()){
				 //String key=((String)to_be_updated_keys.next()).replaceAll("Contact_Reason_Global__r.", "");
				 String key=((String)to_be_updated_keys.next());
				 
					 tobe_updated.put(key, jsonObject.optString(key," "));
				 
			 }
			
			//Description__c,Id,IsDeleted,LastModifiedDate,Name,Status__c
			//String temp_string;
			
			/*//temp_string=jsonObject.getString("Contact_Reason_Autocomplete__c");
			
				tobe_updated.put("Contact_Reason_Autocomplete__c", jsonObject.optString("Contact_Reason_Autocomplete__c"," "));
			

			//temp_string=jsonObject.getString("Id");
			
				tobe_updated.put("Id", jsonObject.optString("Id"," "));
			

			//temp_string=jsonObject.getString("LastModifiedDate");
			
				tobe_updated.put("LastModifiedDate", jsonObject.optString("LastModifiedDate"," "));
			

			//temp_string=jsonObject.getString("Country__c");
		
				tobe_updated.put("Country__c", jsonObject.optString("Country__c"," "));
			

			//temp_string=jsonObject.getString("Default_Priority__c");
		
				tobe_updated.put("Default_Priority__c", jsonObject.optString("Default_Priority__c"," "));
			

			//temp_string=jsonObject.getString("Name");
			
				tobe_updated.put("Name", jsonObject.optString("Name"," "));*/
			

			
			/*tobe_updated.put("Description__c",jsonObject.getString("Description__c") );
			tobe_updated.put("Id",jsonObject.getString("Id") );
			tobe_updated.put("LastModifiedDate",jsonObject.getString("LastModifiedDate") );
			tobe_updated.put("Name",jsonObject.getString("Name") );
			tobe_updated.put("Status__c",jsonObject.getString("Status__c") );*/
			//Datetime 
			
			
			//nestle_dt=Timestamp.valueOf( jsonObject.getString("LastModifiedDate"));
			System.out.println("salesforce modifieddate :"+jsonObject.getString("LastModifiedDate"));
			
			
			
			
			
			//System.out.println("the sf side ms is :"+SF_side_ms);
			
			//DateTime dt = (DateTime)json.deserialize(((String)jsonObject.getString("LastModifiedDate")), DateTime.class);
			//nestle_dt.setTimeZone(TimeZone.getTimeZone("GMT"));
			//nestle_dt.parse(nestle_dt.format(SDF_temp)));
			
			//SDF_temp.format(((String)jsonObject.getString("LastModifiedDate")));
			//nestle_dt=Timestamp.valueOf(SDF_temp.toString());
			//nestle_dt=Timestamp.valueOf((new DateTime(jsonObject.getString("LastModifiedDate"))).toString());
				//System.out.println("the time is "+(new DateTime(jsonObject.getString("LastModifiedDate"))).toString());
			
			if(!rs.next()&&((String)(jsonObject.getString("IsDeleted") ) ).equals("false") ){ //query returns nothing, not in local database record,and incoming IsDeleted=false
				
				//EXCEPTION thrown, NO stamp set!
				try{
				email_receipient(tobe_updated);
				}catch (Exception e){
					testing_enum.EXCEPTION_THROWN.setValue(1);
					System.out.println("################email exception");
				}
				
				//anytime exception thrown, do not insert into database, 
				if(testing_enum.EXCEPTION_THROWN.getValue()==1){
					return null; //break, since this is insertion
				}
				
				//StringBuilder sb=new StringBuilder("insert into nestle_Contact_reason (");
				//String insertion_query="insert into nestle_Contact_reason (";
				
				
				
				//Iterator<?> insertion_keys=jsonObject.keys();
				
				 
				 
				 String prp_stmt_insertion_query=prepared_stmt_insertion_string_builder(jsonObject,tobe_updated);
				 
				 System.out.println("#P#R#P PRP_STMT is :"+prp_stmt_insertion_query);
				 
				 insertion_prp_statement=dbConnection.prepareStatement(prp_stmt_insertion_query);
				 
				 try{
				 insertion_prp_statement=this.<String>inst_stmt_prp(insertion_prp_statement,tobe_updated);
				 
				 }
				 catch(Exception e){
					 System.out.println("# prepared statement exception");
				 }
				 
				 System.out.println("# after appending prp-stmt : "+insertion_prp_statement.toString());
				 
				 //String insertion_query=insertion_string_builder(jsonObject,tobe_updated);
				
				 
				//System.out.println("The new query is :"+insertion_query);
				try{
					insertion_prp_statement.executeUpdate();
					//execution_statement.executeUpdate(insertion_query);
				}catch (SQLException e){
				
					testing_enum.EXCEPTION_THROWN.setValue(1);
					logger.info("insertion SQL errorcode is: "+ e.getErrorCode());
					//can be ignored if deletion occurs in other mule flow
				}
				
				
				//message.setInvocationProperty("delete", false);
			}else if(((String)(jsonObject.getString("IsDeleted") ) ).equals("true")){ //DELETION isDeleted=true and existed
				System.out.println("The to be deleted ID is:" + tobe_updated.get("Id"));
				
				String deletion_query="DELETE FROM nestle_Contact_reason WHERE Id='"+
						jsonObject.getString("Id")+"'";
				
				//The try-catch clause is for safety reason only, probably don't need
				try{
				execution_statement.executeUpdate(deletion_query);
				}catch(SQLException e){
				
					testing_enum.EXCEPTION_THROWN.setValue(1);
					
				}
			}else{ //existed and not deleteion!!! and there must be only one such entry. update local database
				
				if(testing_enum.EXCEPTION_THROWN.getValue()==1){
					return null; //break, since this is insertion
				}
				
				
				//System.out.println("the type of isDeleted is:"+jsonObject.getString("IsDeleted"));
				SimpleDateFormat SDF_local = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");
				SDF_local.setTimeZone(TimeZone.getTimeZone("GMT"));
				
				//java.sql.Timestamp dbsqlTimestamp=rs.getTimestamp("LastModifiedDate");
				
				//System.out.println("string :" + SDF_local.format(dbsqlTimestamp) +" the Id is:"+rs.getString("Id")+"Name:"+rs.getString("Name"));
				
				long local_ms = SDF_local.parse(rs.getString("LastModifiedDate")).getTime();
				//long local_ms = SDF_local.parse(dbsqlTimestamp.toString()).getTime();
				//System.out.println("local modifieddate timestamp :"+dbsqlTimestamp.toString());
				//System.out.println("local modifieddate :"+rs.getString("LastModifiedDate"));
				//System.out.println(" the local time stamp is :"+ local_ms+"the Id is :"+rs.getString("Id"));
				
				
				if(SF_side_ms>local_ms){ //update
					
					StringBuilder update_sb=new StringBuilder("update nestle_Contact_reason SET ");
					
					
					 Iterator<?> update_keys=jsonObject.keys();
					 while(update_keys.hasNext()){
						 //String key=((String)update_keys.next()).replaceAll("Contact_Reason_Global__r.", "");
						 String key=((String)update_keys.next());
						 if(!key.equals("LastModifiedDate")){
							 update_sb.append(key+"='"+(String)tobe_updated.get(key)+"',");
							 //update_sb.append(key+"=\""+(String)tobe_updated.get(key)+"\",");
							 
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
					 
					 /*update_sb.append("LastModifiedDate="+
								"convert(datetimeoffset,"+
								"\""+
								(String)tobe_updated.get("LastModifiedDate")+"\""+
								",127)" //IMPORTANT! salesforce datatype to sql server 'datetime' type must specify type of 127)
								);

						 update_sb.append(
								" WHERE Id=\""+
								(String)tobe_updated.get("Id")+"\"");*/
					 
					 String update_query=update_sb.toString();
					 
					 
					 
					 
					 
					 //disabled update_query
				/*	String update_query="update nestle_Contact_reason SET "+
							"Contact_Reason_Autocomplete__c="+
							"'"+
							(String)tobe_updated.get("Contact_Reason_Autocomplete__c")+"',"+
														
							"LastModifiedDate="+
							"convert(datetimeoffset,"+
							"'"+
							(String)tobe_updated.get("LastModifiedDate")+"'"+
							",127)"+ //IMPORTANT! salesforce datatype to sql server 'datetime' type must specify type of 127
							
							","+
							"Name="+
							"'"+
							(String)tobe_updated.get("Name")+"',"+
							"Country__c="+
							"'"+
							(String)tobe_updated.get("Country__c")+"'," +
							
							"Default_Priority__c="+
							"'"+
							(String)tobe_updated.get("Default_Priority__c")+"' " +
							"WHERE Id='"+
							(String)tobe_updated.get("Id")+"'"
									;*/
					
					logger.info(" The UPDATE query once datestamp greater than is:"+update_query);
					
					try{
						execution_statement.executeUpdate(update_query);
					}catch (SQLException e){
					
					 
			            	testing_enum.EXCEPTION_THROWN.setValue(1);
			            
						logger.info("updating SQL errorcode is: "+ e.getErrorCode());
						//can be ignored if deletion occurs in other mule flow
					}
					
				}
				//local_dt=new DateTime(rs.getString("LastModifiedDate"),DateTimeZone.UTC);
				//DO NOTHING
				//logger.info("deletion:existed");
				//message.setInvocationProperty("delete", true);
				//message.setInvocationProperty("Deletion_Id", modded_data.get("Id"));
				
			} //end of existed but NOT deletion
            
           
            //System.out.println("latitude:" + jsonObject.getBoolean("IsDeleted"));
			
			
        }//end of for json array loop int i
        
        try{
        	dbConnection.close();
        	
        	statement.close();
        	execution_statement.close();
        }catch(Exception e){
        	System.out.println("DBconnection closure error");
        }
        
        //----------------------------------------------------------
        //check in case isDeleted not set but rather Id disappeared (not into recycle bin)
        
        // reverse deletion: 
        /*
        	String inverse_queryString="select * from nestle_invoice_statement";

        	logger.info(" The query String inside deletion is:"+inverse_queryString);
        	ResultSet inv_rs=statement.executeQuery(inverse_queryString); //result set is safe here as previous is finished



        	String to_be_deleted_id;

        	while(inv_rs.next()){

        		boolean delete_flag=true;
        		to_be_deleted_id=inv_rs.getString("Id").replaceAll(" ", ""); //only works when delete_flag=truw

        		for (int i = 0; i < jsonArray.length(); i++) {

        			jsonObject = jsonArray.getJSONObject(i);
        			//System.out.println("Id is :" + jsonObject.getString("Id"));
        			// System.out.println("the json id is"+jsonObject.getString("Id").replaceAll(" ", "")+" the inv id is:"+inv_rs.getString("Id").replaceAll(" ", ""));

        			if(((String)inv_rs.getString("Id").replaceAll(" ", "") ).equals(((String)jsonObject.getString("Id").replaceAll(" ", "")) ) ){
        				delete_flag=false;
        				break; 
        			}
        		}

        		System.out.println("id is:"+inv_rs.getString("Id")+" flag is:"+delete_flag);

        		if(delete_flag==true){
        			//delete 
        			String deletion_query="DELETE FROM nestle_invoice_statement WHERE Id='"+
        					to_be_deleted_id+"'";
        			execution_statement.executeUpdate(deletion_query);
        		}

        	}
        	
        	*/
        
        //---------------end of reverse deletion
        
        
		//JSONObject json = (JSONObject) message.getInvocationProperty("incoming_json");
		//JSONObject json = (JSONObject) JSONSerializer.toJSON(message.getInvocationProperty("incoming_json"));
		
		/*
	    double coolness = json.getDouble( "coolness" );
	    int altitude = json.getInt( "altitude" );
	    JSONObject pilot = json.getJSONObject("pilot");
	    String firstName = pilot.getString("firstName");
	    String lastName = pilot.getString("lastName");
	 
	    System.out.println( "Coolness: " + coolness );
	    System.out.println( "Altitude: " + altitude );
	    System.out.println( "Pilot: " + lastName );
	    */
		/*
		ConsumerIterator<Map<String, Object>> iter =  (ConsumerIterator<Map<String, Object>>) message.getInvocationProperty("incoming_json");
		
		logger.info("reached at iterator parsing");
		int count = 0;
		while (iter.hasNext()) {
			
			logger.info("iterator has next");
			Map<String, Object> sObject = iter.next();
			//returnedSObjectsIds.add(sObject.get("Id").toString());
			count++;
		}
		
		//assertTrue(returnedSObjectsIds.size() > 0);
		assertEquals(count, iter.size());
		*/
		
		/*
		List<?> modded_data=message.getInvocationProperty("incoming_list");
		
		Iterator<?> list_iter=modded_data.iterator();
		int count=0;
		while(list_iter.hasNext()){
			Map<String, Object> sObject = (Map<String,Object>)list_iter.next();
			count++;
		}
		*/
		
		/*
		   assertEquals(count, list_iter.size());
		 */
		
		//CaseInsensitiveHashMap modded_data=message.getInvocationProperty("modded_data");
		
		/*
		Map<String,String> modded_data=message.getInvocationProperty("modded_data");
		String data_in_payload=modded_data.toString();
		message.setInvocationProperty("data_string", data_in_payload);
		
		logger.info("modded id is :" + modded_data.get("Id"));
		
		logger.info("the dbhost is :"+dbhost +" the authinfo is :"+authinfo+ " the dbname is "+ dbname);
		dbConnection = DBConnection.getConnection(dbhost, authinfo, dbname);
		Statement statement=dbConnection.createStatement();
		
		if(modded_data.get("IsDeleted")==null){ //when query hashmap has no IsDeleted present, then it's a deletion, otherwise IsDeleted key presents and will be false TODO:why not showing true?
			
			message.setInvocationProperty("deletion_choice", true); 
			
			String queryString="select * from nestle_invoice_statement where Id='"+modded_data.get("Id")+"'";
			
			logger.info(" The query String inside deletion is:"+queryString);
			ResultSet rs=statement.executeQuery(queryString);
			
			if(!rs.next()){ //query returns nothing, not in local database record, do not delete
				message.setInvocationProperty("delete", false);
			}else{
				logger.info("deletion:existed");
				message.setInvocationProperty("delete", true);
				//message.setInvocationProperty("Deletion_Id", modded_data.get("Id"));
			}
			
		}else{ // "IsDeleted" is not null (most likely false) not deletion but rather update or insertion or even undelete(same as insertion and update)
			
			message.setInvocationProperty("deletion_choice", false);
			
			
			String queryString="select * from nestle_invoice_statement where Id='"+modded_data.get("Id")+"'";

			logger.info(" The query String inside update and insertion is:"+queryString);
			
			ResultSet rs=statement.executeQuery(queryString);

			if(!rs.next()){ //if query is empty, insert
				logger.info("the updated data is brand new\n");
				message.setInvocationProperty("existing", false);
			}else{ //TODO: modifiy/update existing data entry
				logger.info("update:existed");
				message.setInvocationProperty("existing", true);
				message.setInvocationProperty("existing_Id", modded_data.get("Id"));
			}
		}
		
		*/
		// 
		System.out.println( "datepath is : " + datepath );
		
		//CAUTION! should not reach write_date_stamp should database exception threw
		if(testing_enum.EXCEPTION_THROWN.getValue()==0){ //0 NOT thrown, thus write date stamp
			write_date_stamp();
		}
		
		
		testing_enum.EXCEPTION_THROWN.setValue(0);
		
		
		
		testing_enum.TESTING_SINGLETON.setValue(100);//TESTING only
		return null;
		
		
	}
	
	private void email_receipient(Map<String,?> receipient_hashmap) throws Exception{
		
		
		//final String username = "username@gmail.com";
		//final String password = "password";

		if(
				((Float.valueOf((String)receipient_hashmap.get("Level__c")).intValue())!=3)
				||
				(!((String)receipient_hashmap.get("Name")).toLowerCase().contains("Injuries".toLowerCase()))
				){ //Level is not 3, return without sending
			return;
		}
		String injection_smtp_user=this.smtp_user;
		String injection_smtp_password=this.smtp_password;
		
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", this.smtp_host);
		props.put("mail.smtp.port", this.smtp_port);

		Session session = Session.getInstance(props,
		  new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				PasswordAuthentication pa= new PasswordAuthentication(injection_smtp_user, injection_smtp_password);
				
				/*if(pa==null){
					testing_enum.EXCEPTION_THROWN.setValue(1); //NOT update timestamp
				}*/
					
				
				
				return pa;
			}
		  });

		System.out.println("the session is: "+session.toString());
		//try { temporarily disabled exception

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("clifford.zheng@itsp-inc.com"));
			message.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse("clifford.zheng@itsp-inc.com"));
				//InternetAddress.parse("clifford.zheng@itsp-inc.com,terrence.wong@itsp-inc.com,Ana.Marica@ca.nestle.com,william.chen@millennium1solutions.com,James.berry@millennium1solutions.com"));
			message.setSubject("New Case with alert");
			
			
			String msg_body=new String();
			
			
			//disabled
		  /*  for(Map.Entry<String, Object>entry: receipient_hashmap.entrySet()){
		    	//String temp_string=entry.getKey().replaceAll("Contact_Reason_Global__r.", "")+" :    "+entry.getValue().toString();
		    	String temp_string=entry.getKey()+" :    "+entry.getValue().toString();

		    	if(temp_string.contains("Id")||
		    			temp_string.contains("LastModifiedDate")||
		    			temp_string.contains("ID")||
		    			temp_string.contains("IsDeleted")
		    			){ //ignored
		    		continue;
		    	}
		    			temp_string=temp_string.replace("__c", "");
		    	msg_body+=temp_string+"\n";
		    }*/
		    
		    
		    msg_body="Case Subject:   "+(String)receipient_hashmap.get("Subject")+"\n\n";
		    msg_body+="Case Description:\n"+(String)receipient_hashmap.get("Description")+"\n\n";
		    msg_body+="Region:  "+(String)receipient_hashmap.get("Country__c")+"\n";
		    msg_body+="Contact Reason:  "+(String)receipient_hashmap.get("Name")+"\n";
		    msg_body+="Priority:   "+(String)receipient_hashmap.get("Default_Priority__c")+"\n";
		    msg_body+="Contact Qualification:    "+ (String)receipient_hashmap.get("Contact_Qualification__c")+"\n";
		    msg_body+="Reason Level:   "+(Float.valueOf((String)receipient_hashmap.get("Level__c")).intValue())+"\n\n\n\n";
		    msg_body+="Reason Level1 -- "+(String)receipient_hashmap.get("Level1__c")+"\n";
		    msg_body+="      |\n";
		    msg_body+="       ---Reason Level2 -- "+(String)receipient_hashmap.get("Level2__c")+"\n";
		    msg_body+="                |\n";
		    msg_body+="                 ---Reason Level3 -- "+(String)receipient_hashmap.get("Level3__c")+"\n";
		    
			message.setText(msg_body);

			Transport.send(message);

			System.out.println("Done");

		//} catch (MessagingException e) {
		//	testing_enum.EXCEPTION_THROWN.setValue(1);
		//	throw new RuntimeException(e);
		//}
		
		
	}
	
	private String special_char_removal(String input){
		
		return input;
		
		//return input.replace("'", "''").replace("\n", " ").replace("\r", " ");
		
		//replace("\\", "|").replace("/", "|");
		
		/*CharSequence target="'";
		
		CharSequence replacement="''";
		
		if(input.contains("'")){
			return input.replace(target, replacement);
		}else{
			return input;
		}
		*/
		//return null;
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
		//Iterator<?> insertion_keys=jsonObject.keys();
		int index=1;
		
		for(Map.Entry<String,? super E> entry: tobe_updated.entrySet()){
			
			if(!(entry.getKey().equals("LastModifiedDate"))){
				prp_stmt.setString(index, entry.getValue().toString());
			}else{
				SimpleDateFormat SDF_SF_side=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				SDF_SF_side.setTimeZone(TimeZone.getTimeZone("GMT"));
				
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
				
				System.out.println("###The LastModifiedDate is "+entry.getValue());
				prp_stmt.setTimestamp(index, new Timestamp( 
						
						
						//(new java.util.Date()).getTime()
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
	
	//abandoned favor of prepared statement insertion method
	private <E> String insertion_string_builder(JSONObject jsonObject,LinkedHashMap<String,E> tobe_updated){
		Iterator<E> insertion_keys=jsonObject.keys();
		
		StringBuilder insertion_sb_fields=new StringBuilder("insert into nestle_Contact_reason (");
		StringBuilder insertion_sb_values=new StringBuilder(" values(");
		
		 while(insertion_keys.hasNext()){
			 //String key=((String)insertion_keys.next()).replaceAll("Contact_Reason_Global__r.", "");
			 String key=((String)insertion_keys.next());
			 if(!key.equals("LastModifiedDate")){
				 insertion_sb_fields.append(key+",");
				 insertion_sb_values.append("'"+(String)tobe_updated.get(key)+"',");
				// insertion_sb_values.append("\""+(String)tobe_updated.get(key)+"\",");
			 }
			 
		 }
		 
		 //Modified date requires special convertion
		 insertion_sb_fields.append("LastModifiedDate)");
		 
		 insertion_sb_values.append("convert(datetimeoffset,"+
					"'"+
					(String)tobe_updated.get("LastModifiedDate")+"'"+
					",127))" //IMPORTANT! salesforce datatype to sql server 'datetime' type must specify type of 127
					);
		 
		/* insertion_sb_values.append("convert(datetimeoffset,"+
					"\""+
					(String)tobe_updated.get("LastModifiedDate")+"\""+
					",127))" //IMPORTANT! salesforce datatype to sql server 'datetime' type must specify type of 127
					);*/
		 
		 return insertion_sb_fields.toString()+insertion_sb_values.toString();
	}
	
	private void write_date_stamp() throws Exception{
		
		Calendar calendar = current_calendar; // gets a calendar using the default time zone and locale.
		calendar.add(Calendar.SECOND, testing_enum.TIME_STAMP_ROLLBACK_SECS.getValue());
		System.out.println("current date is:"+calendar.getTime());
		
		DateFormat date_format_for_store=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		date_format_for_store.setTimeZone(TimeZone.getTimeZone("GMT"));
		
			System.out.println("parsed date is "+date_format_for_store.format(calendar.getTime()));
		
		
		 try{
	            //String verify, putData;
	            String filename=this.datepath+"lastmodded_date.txt";
	            File file = new File(filename);
	            
	            
	            file.createNewFile();
	            FileWriter fw = new FileWriter(file);
	            BufferedWriter bw = new BufferedWriter(fw);
	            bw.write(date_format_for_store.format(calendar.getTime()));
	            bw.flush();
	            bw.close();
	            
	            
	           /* FileReader fr = new FileReader(file);
	            BufferedReader br = new BufferedReader(fr);

	            while( (verify=br.readLine()) != null ){ //***editted
	                       //**deleted**verify = br.readLine();**
	                if(verify != null){ //***edited
	                    putData = verify.replaceAll("here", "there");
	                    bw.write(putData);
	                }
	            }
	            br.close();*/


	        }catch(IOException e){
	        	
	        e.printStackTrace();
	        }
	    }

	

}

