package mrjob;

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
	private String zookeeper;
	@Override
	public void map(Text key,Text value,Context context)
	{
		LOG.info("start map! for key= "+key.toString());
		try {
			zookeeper=context.getConfiguration().get("zookeeper");
			LOG.info("preparing to connect phoenix");
			Connection cn=DriverManager.getConnection("jdbc:phoenix:"+zookeeper);
			LOG.info("connect to phoenix: "+"jdbc:phoenix:"+zookeeper);
			String []lines=value.toString().split("\n");
			for(String oneLine : lines)
			{
				LOG.info("Mapper Line: "+oneLine);
				PreparedStatement stmt=cn.prepareStatement("UPSERT INTO MRTABLE(file_name,id,val) VALUES(?,?,?)");
				String []keyValue=oneLine.split(" ");
				stmt.setInt(2, Integer.parseInt(keyValue[0]));
				stmt.setInt(3, Integer.parseInt(keyValue[1]));
				stmt.setString(1,key.toString());
				stmt.executeUpdate();
			}
			cn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
