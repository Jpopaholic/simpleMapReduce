package mrjob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LocalFileInputFormat extends InputFormat<Text,Text> 
{
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		SingleFileInputSplit oneSpilt=(SingleFileInputSplit) arg0;
		RecordReader<Text, Text>result=new FileContextRecordReader(new Text(oneSpilt.getFileName()),new Text(oneSpilt.getFileContext()));
		return result;
	}

	private static final int SPLIT_ROWS=100000;
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
			   Configuration conf=arg0.getConfiguration();
		        List<InputSplit> out=new ArrayList<InputSplit>();
		        File dir=new File(conf.get("filedir"));
		        File[] files=dir.listFiles();
	            for(File singleFile : files)
	            {
	            	if(!singleFile.exists())throw new IOException("File Not Found");
	            	else{
					            	BufferedReader fin = new BufferedReader(new FileReader(singleFile));
					            	String line;
					            	int flush=1;
					            	StringBuilder sb=new StringBuilder();
					            	while((line=fin.readLine())!=null)
					            	{
					            		sb.append(line+"\n");
					            		if(flush%SPLIT_ROWS==0)
					            		{
					            			out.add(new SingleFileInputSplit(singleFile.getName(),sb.toString(),flush));
					            			sb.delete(0, sb.length());
					            			flush=0;
					            		}
					            		flush++;
					            	}
					            	//final split
					            	if(sb.length()>0)out.add(new SingleFileInputSplit(singleFile.getName(),sb.toString(),flush-1));
					            	sb.delete(0, sb.length());
				            		fin.close();
	            	}
	            }
		return out;
	}
	
}
