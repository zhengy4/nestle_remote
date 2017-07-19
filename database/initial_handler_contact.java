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

public class initial_handler_contact implements Callable{
	
	private static Log logger = LogFactory.getLog(initial_handler.class);
	
	private String datepath;
	
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
		
		//System.out.println("initilization");
		// TODO Auto-generated method stub
		
		//Thread.sleep(30000);
		//get last timestamp in order to not overload initial salesforce selection
		 String filename=this.datepath+"contact_lastmodded_date.txt";
         File file = new File(filename);
         
         FileReader fr = new FileReader(file);
         BufferedReader br = new BufferedReader(fr);
         
         String date_stamp_temp=br.readLine();
         //System.out.println("initilization"+ br.readLine());
         //logger.info("initilization:"+ date_stamp_temp);
         
         eventContext.getMessage().setInvocationProperty("last_time_stamp",new String(date_stamp_temp));
         
         return date_stamp_temp; //to go around failed setinvocationproperty
		//return eventContext.getMessage().getPayload();
		//return null;
	}

}