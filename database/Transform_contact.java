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

	
	
	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		
		Connection dbConnection = null;
		
		ResultSet rs;
		
		
		MuleMessage message = eventContext.getMessage();
		
		
		
		return null;
	}
	
	

}
