package com.bt.taas.mapreduce;
/**
 * 
 */

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 609504664
 *
 */
public class RetOriginalValueDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// int res = ToolRunner.run(new Configuration(), new TokenRetDriver(),
		// args);
		int res = ToolRunner.run(new RetOriginalValueDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		File f1 = new File(args[2]);
		File f2 = new File(args[3]);
		Configuration conf = getConf();
		conf.set("delimiter", args[0]);
		conf.set("indexvalue", args[1]);
		//conf.set("prefix", args[2]);
		//conf.set("postfix", args[3]);
		conf.set("configfilename", f1.getName());
		conf.set("jksfilename", f2.getName());
		Job job = Job.getInstance(conf);
		job.setJarByClass(RetOriginalValueDriver.class);
		job.setMapperClass(RetOriginalValueMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);
		DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3]), job.getConfiguration());
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
