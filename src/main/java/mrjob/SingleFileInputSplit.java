package mrjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class SingleFileInputSplit extends InputSplit implements  Writable{
	private static final Log LOG = LogFactory.getLog(SingleFileInputSplit.class);
	private String fileName;
	private String fileContext;
	private int linecount;
	public SingleFileInputSplit()
	{
		super();
	}
	public SingleFileInputSplit(String fileName,String fileContext,int linecount)
	{
		this.fileName=fileName;
		this.fileContext=fileContext;
		this.linecount=linecount;
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
		out.writeInt(linecount);
		out.writeBytes(fileName+"\n");
		out.writeBytes(fileContext);
		
	}
	public void readFields(DataInput in) throws IOException {
		int lineCount=in.readInt();
		fileName=in.readLine();
		StringBuilder sb=new StringBuilder();
		String line;
		for(int i=0;i<lineCount;i++)
		{
			line=in.readLine();
			sb.append(line+"\n");
		}
		fileContext=sb.toString();
		
	}
	public static SingleFileInputSplit read(DataInput in)throws IOException{
		SingleFileInputSplit oneSplit= new SingleFileInputSplit();
		oneSplit.readFields(in);
		return oneSplit;
	}

}
