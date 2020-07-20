/**
 * 
 */
package com.bt.taas.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 609504664
 *
 */
public class TokenRetDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// int res = ToolRunner.run(new Configuration(), new TokenRetDriver(),
		// args);
		int res = ToolRunner.run(new TokenRetDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		File f1 = new File(args[4]);
		File f2 = new File(args[5]);
		Configuration conf = getConf();
		conf.set("delimiter", args[0]);
		conf.set("indexvalue", args[1]);
		conf.set("prefix", args[2]);
		conf.set("postfix", args[3]);
		conf.set("configfilename", f1.getName());
		conf.set("jksfilename", f2.getName());
		Job job = Job.getInstance(conf);
		job.setJarByClass(TokenRetDriver.class);
		job.setMapperClass(TokenRetMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);
		DistributedCache.addCacheFile(new URI(args[4]), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[5]), job.getConfiguration());
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[6]));
		FileOutputFormat.setOutputPath(job, new Path(args[7]));
		if(args[8].equalsIgnoreCase("1")){
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}else if (args[8].equalsIgnoreCase("2")){
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);	
		}
		//DistributedCache.addCacheFile(new URI(args[8]), job.getConfiguration());
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
