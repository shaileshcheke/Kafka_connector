package org.alitix.connect.jdbc;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.sql.Connection;
import com.mysql.jdbc.Driver;

public class JDBCSourceConnector extends SourceConnector{
	public static final String TOPIC_CONFIG = "topic";
	public static final String DATABASE_CONFIG = "database.connection.url";
	public static final String TABLE_NAME="table.name";
	public static final String CUSTOM_QUERY="custom.query";
	public static final String INCREMENT_COLUMN="increment.column";
	public static final String POLL_INTERVAL="poll.interval";
	private String topic;
	private String database;
	private String tablename;
	private String custom_query;
	private String increment_column;
	private String poll_interval;
 
	@Override
	public ConfigDef config() {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * Load configuration from Map received through kakfa connect 
	 */
	@Override
	public void start(Map<String, String> props) {
		database= props.get(DATABASE_CONFIG);
	    topic = props.get(TOPIC_CONFIG);
	    if(props.containsKey(TABLE_NAME))
	    	tablename=props.get(TABLE_NAME);
	    if(props.containsKey(CUSTOM_QUERY))
	    	custom_query=props.get(CUSTOM_QUERY);
	    if(props.containsKey(INCREMENT_COLUMN))
	    	increment_column=props.get(INCREMENT_COLUMN);
	    if(props.containsKey(POLL_INTERVAL))
	    	poll_interval=props.get(POLL_INTERVAL);
	  
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<? extends Task> taskClass() {
		// TODO Auto-generated method stub
		return JDBCSourceTask.class;
	}
	/**
	 * Passes configuration to SourceTask.
	 */
	@Override
	public List<Map<String, String>> taskConfigs(int arg0) {
		// TODO Auto-generated method stub
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		Map<String, String> config = new HashMap<>();
		if (database != null)
			config.put(DATABASE_CONFIG, database);
		config.put(TOPIC_CONFIG, topic);
		if (tablename !=null )
		    config.put(TABLE_NAME, tablename);
		if(custom_query !=null)
		    config.put(CUSTOM_QUERY, custom_query);
		if(increment_column != null)
			config.put(INCREMENT_COLUMN,increment_column);
		if(poll_interval !=null)
			config.put(POLL_INTERVAL,poll_interval);
		
		configs.add(config);
		return configs;
		
	}

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

}
