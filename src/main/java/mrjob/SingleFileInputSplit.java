package mrjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class SingleFileInputSplit extends InputSplit implements  Writable{

	private String fileName;
	private String fileContext;
	public SingleFileInputSplit()
	{
		super();
	}
	public SingleFileInputSplit(String fileName,String fileContext)
	{
		this.fileName=fileName;
		this.fileContext=fileContext;
	}
	public String getFileName()
	{
		return fileName;
	}
	public String getFileContext()
	{
		return fileContext;
	}
	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return fileContext.length();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] nullString=new String[1];
		nullString[0]=new String();
		return nullString;
	}
	public void write(DataOutput out) throws IOException {
		out.writeBytes(fileName);
		out.writeBytes("\n");
		out.writeBytes(fileContext);
		
	}
	public void readFields(DataInput in) throws IOException {
		fileName=in.readLine();
		String line;
		StringBuilder sb=new StringBuilder();
		boolean first=true;
		while((line=in.readLine())!=null)
		{
			if(first)
			{
				sb.append(line);
				first=false;
			}
			else
			{
				sb.append("\n");
				sb.append(line);
			}
		}
		fileContext=sb.toString();
		
	}

}
