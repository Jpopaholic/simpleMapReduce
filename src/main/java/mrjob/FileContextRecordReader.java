package mrjob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FileContextRecordReader  extends RecordReader<Text, Text>{

	private Text key;
	private Text value;
	private boolean canRead;
	private Configuration conf;
	public FileContextRecordReader(Text key,Text value,Configuration conf)
	{
		this.key=key;
		this.value=value;
		this.conf=conf;
		canRead=true;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(!conf.getBoolean("success", false))
		{
			String error=conf.get("error message");
			throw new InterruptedException(error);
		}
		else return 1;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(canRead)
			{
				canRead=false;
				return true;
			}
		return false;
	}

}
