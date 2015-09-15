package mrjob;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class FileMapper extends Mapper<Text,Text,Text,Text>{
	private static final Log LOG = LogFactory.getLog(FileMapper.class);
	private Connection cn;
	@Override
	  protected void setup(Context context
              ) throws IOException, InterruptedException {
		String zookeeper=context.getConfiguration().get("zookeeper");
		LOG.info("preparing to connect phoenix : " + zookeeper);
		try {
			cn =DriverManager.getConnection("jdbc:phoenix:"+zookeeper);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to create sql conenction", e);
		}
	}
	@Override
	public void map(Text key,Text value,Context context)
	{
		LOG.info("start map! for key= "+key.toString());
		try {
			String []lines=value.toString().split("\n");
			PreparedStatement stmt=cn.prepareStatement("UPSERT INTO MRTABLE(file_name,id,val) VALUES(?,?,?)");
			for(String oneLine : lines)
			{
				LOG.info("Mapper Line: "+oneLine);				
				String []keyValue=oneLine.split(" ");
				stmt.setInt(2, Integer.parseInt(keyValue[0]));
				stmt.setInt(3, Integer.parseInt(keyValue[1]));
				stmt.setString(1,key.toString());
				stmt.addBatch();
			}
			stmt.executeBatch();
			cn.commit();
		} catch (SQLException e) {
			LOG.error("Failed to open sql connection", e);
		}
	}
	@Override
	  protected void cleanup(Context context
              ) throws IOException, InterruptedException {
		try {
			cn.close();
		} catch (SQLException e) {
			throw new IOException(e);
		}
}
}
