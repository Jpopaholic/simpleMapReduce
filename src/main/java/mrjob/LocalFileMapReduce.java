package mrjob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class LocalFileMapReduce {
	public static void main(String []argv) throws IOException, ClassNotFoundException, InterruptedException
	{
			Configuration conf;
			String []remaningArgv=null;
			GenericOptionsParser gop=new GenericOptionsParser(argv);
			remaningArgv=gop.getRemainingArgs();
		if(remaningArgv==null||remaningArgv.length!=2)
		{
			System.err.println("usage: [file director] [zookeeper name]");
		}
		else
		{
			conf=new Configuration();
			conf.set("filedir", remaningArgv[0]);
			conf.set("zookeeper", remaningArgv[1]);
				Job jb=Job.getInstance(conf,"TESTMR");
				jb.setJarByClass(LocalFileMapReduce.class);
				jb.setInputFormatClass(LocalFileInputFormat.class);
				jb.setMapperClass(FileMapper.class);
				jb.setOutputFormatClass(NullOutputFormat.class);
				jb.setNumReduceTasks(0);
				jb.submit();
		}
	}
}