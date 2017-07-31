package org.alitix.connect.jdbc;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.pict.connect.jdbc.JDBCSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;


public class JDBCSourceTask extends SourceTask {
	//logger for logging activity 
	private static final Logger log = LoggerFactory.getLogger(JDBCSourceTask.class);
	public static final String TABLENAME_FIELD = "table";
	public static final String OFFSET_VALUE="offset";
	private String database=null;
	private String topic=null;
	private String table=null;
	private String custom_query=null;
	private String increment_column=null;
	private long increment_offset=-1;
	private boolean increment_colu=false;
	private Connection cn;
	private Schema schema;
	private SchemaBuilder schemabuilder;
	private Statement st;
	private long lastupdate;
	private Time tm;
	private long poll_interval;
	Map<String,String> columninfo=new HashMap<>();
	
	public JDBCSourceTask() {
		// TODO Auto-generated constructor stub
		tm=	new SystemTime();
	}
	
	@Override
	public String version() {
		return null;
	}
   
	
	public Map<String,String> getColumnInfo()
	{	
		return columninfo;
	}
	
	/**
	 * Calculate offset which will be committed to zoopkeeper.
	 * @throws SQLException
	 */
	
	public void calculateOffset() throws SQLException
	{
		if(increment_colu)
		{
			st.execute("select max("+increment_column+") as max from "+table+";");
			ResultSet sr=st.getResultSet();
			sr.next();
			increment_offset=sr.getLong("max");
			sr.close();
			sr=null;
		}
		
	}
	
	/**
	 * Create and return struct form row. 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	
	public Struct createStruct(ResultSet rs) throws SQLException
	{
		
		Struct st1=new Struct(schema);
		for(Entry<String, String> e1:columninfo.entrySet())
		{
			switch(e1.getValue()){
				case "INT":
					st1.put(e1.getKey(), rs.getInt(e1.getKey()));
					if(increment_colu)
						{
							if(e1.getKey().equals(increment_column))
							{
								increment_offset=rs.getInt(e1.getKey());	
							}		
						}
					
					break;
				case "VARCHAR":
					st1.put(e1.getKey(), rs.getString(e1.getKey()));
					break;
				
				}
			
		}
		return st1;
	}
	
	/**
	 * Create and Execute database query based on configuration specified.
	 * @return
	 * @throws SQLException
	 */
	
	
	public  ResultSet executeQuery() throws SQLException
	{
		if(custom_query != null && increment_colu)
		{
			st.execute("select "+custom_query+" from "+table+" where "+increment_column+">"+increment_offset+";");
		}
		else if (custom_query != null){
			st.execute("select "+custom_query+" from "+table+";");
		}
		else if(increment_colu){
			st.execute("select * from "+table+" where "+increment_column+">"+increment_offset+";");
		}
		else{
			st.execute("select * from "+table+";");
		}
		return st.getResultSet();
		
		
	}
	
	/**
	 * Executed by kafka periodically.
	 */
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// TODO Auto-generated method stub
		long now=tm.milliseconds();
		long diff=(lastupdate+poll_interval)-now;
		if(diff>0)
		{
			tm.sleep(diff);
			
		}
		/*Offset retrive from the code*/
		synchronized (JDBCSourceTask.class) {
			if(increment_colu)
			{
				Map<String,Object> dt=context.offsetStorageReader().offset(Collections.singletonMap(TABLENAME_FIELD, table));
				if(dt==null)
				{
					increment_offset=0L;		
				}
				else{	
					Object lastIndex=dt.get(OFFSET_VALUE);
					if(lastIndex !=null)
					{
						increment_offset=(long)Long.valueOf((Long)lastIndex);
				
					}	
				}
			}
		List<SourceRecord> rows=new ArrayList<SourceRecord>();
		ResultSet rs;
		try{  
			rs=executeQuery();
			if(rs ==null)
				return null;
			while(rs.next()){
				Struct st1=createStruct(rs);
				rows.add(new SourceRecord(getOffsetKey(table), getOffsetValue(increment_offset), topic, st1.schema(),st1));
			}
			rs.close();
			rs=null;
			lastupdate=tm.milliseconds();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}
		return rows;
	  }
		
	}
	
	/**
	 * Returns data type for the field of struct.
	 * @param type
	 * @return
	 */
    public Schema getFieldType(String type)
    {
    	switch(type)
    	{
    		case "INT":
    			return Schema.INT32_SCHEMA;
    		
    		case "VARCHAR":
    			return Schema.STRING_SCHEMA;
    		default:
    			throw new UnknownError("It must be a New type not handled in code");
    		
    	}
    	
    }
    
    
    /**
     * Build schema from table description.
     * @param cn
     * @param custom_query
     */
	public void buildSchema(Connection cn,String custom_query)
	{
		boolean custquery=false;
		List<String> l1= new ArrayList<String>();
		if(custom_query !=null)
		{
			custquery=true;
			String[] tokens=custom_query.split(",");
			for(String s:tokens){
				l1.add(s);
			}
		}
		try
		{
			DatabaseMetaData meta=cn.getMetaData();
			ResultSet rs=meta.getColumns(null, null, table, null);
			schemabuilder=SchemaBuilder.struct().name(table);
			while(rs.next())
			{
				String columnn=rs.getString("COLUMN_NAME");
				if(custquery && !l1.contains(columnn))
					continue;
				String columnty=rs.getString("TYPE_NAME");
				columninfo.put(columnn, columnty);
				schemabuilder.field(columnn, getFieldType(columnty));
			}
			schema=schemabuilder.build();
			rs.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}
		
	}
	
	
	/**
	 * SourceTask starts here.
	 */
	@Override
	public void start(Map<String, String> conf) {
		// TODO Auto-generated method stub
		poll_interval=(conf.containsKey(JDBCSourceConnector.POLL_INTERVAL)) ? Long.parseLong(conf.get(JDBCSourceConnector.POLL_INTERVAL)) : 500;
		database=conf.get(JDBCSourceConnector.DATABASE_CONFIG);
		if (database==null)
			try {
				throw new ConnectException("Database is not specified");
			} catch (ConnectException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		topic=conf.get(JDBCSourceConnector.TOPIC_CONFIG);
		table=conf.get(JDBCSourceConnector.TABLE_NAME);
		if (table==null)
			try {
				throw new ConnectException("Table is not specified");
			} catch (ConnectException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		if(conf.containsKey(JDBCSourceConnector.CUSTOM_QUERY))
			custom_query=conf.get(JDBCSourceConnector.CUSTOM_QUERY);
		if(conf.containsKey(JDBCSourceConnector.INCREMENT_COLUMN)){
				increment_column=conf.get(JDBCSourceConnector.INCREMENT_COLUMN);
				increment_colu=true;
		}
		try{
			cn=DriverManager.getConnection(database);
			st=cn.createStatement();
			buildSchema(cn,custom_query);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		lastupdate=0;
	}
	
	
	/**
	 * Cleaning and freeing resources.
	 */
	@Override
	public void stop() 
	{
		// TODO Auto-generated method stub
		try{
			st.close();
			cn.close();
			schema=null;
			schemabuilder=null;
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}

	}
	public Map<String,String> getOffsetKey(String table_name)
	{
		return Collections.singletonMap(TABLENAME_FIELD, table_name);
		
	}
	public Map<String,Long> getOffsetValue(long offset)
	{
		return Collections.singletonMap(OFFSET_VALUE,new Long(offset));
		
	}

}
