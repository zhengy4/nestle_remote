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
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Time;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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


//import com.m1s.filegenerator.GenEnrollmentFile;
//import com.m1s.tool.DBConnection;
//import com.m1s.tool.EncryptCardNo;
//import  com.m1s.tool.Utils;

public class DeletionIncomingData implements Callable {
	
private static Log logger = LogFactory.getLog(TransformIncomingData.class);
	
	private String dbhost;
	private String authinfo;
	//private String dbuser;
	//private String dbpass;
	private String dbname;
	
	
	
	
	public enum testing_enum{A_VALUE(1),B_VALUE(2),MAX_COUNTER_BEFORE_DELETION(15);
		private int value;
		
		private testing_enum(int value){
			this.value=value;
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

	/*public String getDatepath() {
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
	}*/


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
		
	
				
		Connection dbConnection = null;
		

		MuleMessage message = eventContext.getMessage();
	
		
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
		String json = (message.getInvocationProperty("deletion_json"));
        //System.out.println("JSON_VALUE"+message.getInvocationProperty("incoming_json"));
        JSONArray jsonArray = new JSONArray(json);
        JSONObject jsonObject;
        
        try{
        dbConnection = DBConnection.getConnection(dbhost, authinfo, dbname);
        }catch(Exception e){
        	System.out.println("deletion JDBC exception");
        }
		Statement statement=dbConnection.createStatement(); //rs(result set) loop only
		
		Statement execution_statement=dbConnection.createStatement();
		
		
        	String inverse_queryString="select * from nestle_Contact_reason";

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
        			String deletion_query="DELETE FROM nestle_Contact_reason WHERE Id='"+
        					to_be_deleted_id+"'";
        			execution_statement.executeUpdate(deletion_query);
        		}

        	}
        	
        	
		
		
        	
        	
        	 try{
             	dbConnection.close();
             	
             	statement.close();
             	execution_statement.close();
             }catch(Exception e){
             	System.out.println("DBconnection closure error");
             }
             
		
		
		return null;
	}

}
