package main;

import ioformat.MyInputFormat;
import ioformat.MyOutputFormat;
import mapreduce.SocialCombiner;
import mapreduce.SocialMapper;
import mapreduce.SocialReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("training_file", args[0]);
	    conf.set("test_file",args[1]);
	    conf.set("output_file",args[2]);
	    Job job = Job.getInstance(conf, "Social");
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path out = new Path(args[2]);
	    FileSystem.get(conf).delete(out, true);
        FileOutputFormat.setOutputPath(job, out);
        
	    job.setJarByClass(Main.class);
	    job.setMapperClass(SocialMapper.class);
	    job.setCombinerClass(SocialCombiner.class);
	    job.setReducerClass(SocialReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
