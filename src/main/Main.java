package main;

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
	    conf.set("knn.k",args[3]);
	    Job job = Job.getInstance(conf, "kNN");
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path out = new Path(args[2]);
	    FileSystem.get(conf).delete(out, true);
        FileOutputFormat.setOutputPath(job, out);
        
//	    job.setJarByClass(Main.class);
//	    job.setMapperClass(kNNMapper.class);
//	    job.setCombinerClass(kNNCombiner.class);
//	    job.setReducerClass(kNNReducer.class);
//	    job.setMapOutputKeyClass(IdAndData.class);
//	    job.setMapOutputValueClass(DistanceAndClass.class);
//	    job.setOutputKeyClass(IdAndData.class);
//	    job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(MyInputFormat.class);
//        job.setOutputFormatClass(MyOutputFormat.class);
        
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
