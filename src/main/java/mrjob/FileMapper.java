package mrjob;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileMapper extends Mapper<Text,Text,Text,Text>{
	private String zookeeper;
	@Override
	public void map(Text key,Text value,Context context)
	{
		try {
			zookeeper=context.getConfiguration().get("zookeeper");
			Connection cn=DriverManager.getConnection("jdbc:phoenix:"+zookeeper);
			String []lines=value.toString().split("\n");
			for(String oneLine : lines)
			{
				PreparedStatement stmt=cn.prepareStatement("UPSERT INTO MRTABLE(file_name,id,val) VALUES(?,?,?)");
				String []keyValue=oneLine.split(" ");
				stmt.setInt(2, Integer.parseInt(keyValue[0]));
				stmt.setInt(3, Integer.parseInt(keyValue[1]));
				stmt.setString(1,key.toString());
				stmt.executeUpdate();
			}
			cn.commit();
			PreparedStatement query=cn.prepareStatement("select count(*) from MRTABLE");
			ResultSet rs=query.executeQuery();
			if(rs.getInt(1)>0)context.getConfiguration().setBoolean("success", true);
			else
				{
					context.getConfiguration().setBoolean("success", false);
					context.getConfiguration().set("error message", "ERROR: key= "+key.toString()+" value= "+value.toString());
				}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			context.getConfiguration().setBoolean("success", false);
			context.getConfiguration().set("error message", e.toString());
			e.printStackTrace();
		}
	}

}
