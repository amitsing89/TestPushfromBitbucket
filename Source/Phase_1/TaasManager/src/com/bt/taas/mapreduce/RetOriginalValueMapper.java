package com.bt.taas.mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bt.taas.httpProcessor.HttpProcessor;

/**
 * @author 609504664
 *
 */
public class RetOriginalValueMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	String authkey = "";
	HttpProcessor tokenret = null;
	//String jksfile = "";
	Text text = new Text();
	String delimiter = "";
	String prefix = "";
	String postfix = "";
	// int index=0;
	List<Integer> index = new ArrayList<>();
	String configfilename = "";
	String jksfilename = "";
	String filename = "";
	MultipleOutputs<NullWritable, Text> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<>(context);
		tokenret = new HttpProcessor();
		// String filepath =context.getConfiguration().get("config-file");
		//String configfileName = null;
		//String fileName = null;
		delimiter = context.getConfiguration().get("delimiter");
		for (String li : context.getConfiguration().get("indexvalue").split(",")) {
			index.add(Integer.parseInt(li));
		}
		// index=Integer.parseInt(context.getConfiguration().get("index"));
		configfilename = context.getConfiguration().get("configfilename");
		jksfilename = context.getConfiguration().get("jksfilename");

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		filename = fileSplit.getPath().getName();

		/*Path[] path = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath : path) {
			fileName = eachPath.getName().toString().trim();
			if (fileName.equalsIgnoreCase(configfilename)) {
				configfileName = fileName;
			}
			if (fileName.equalsIgnoreCase(jksfilename)) {
				jksfile = fileName;
			}

		}*/
		//System.out.println("fileName " + configfileName + "Path " + pathName);
		//System.out.println("jksfile " + jksfile);
		tokenret.setProperties(configfilename);
		authkey = tokenret.getAuthKey(jksfilename);
		//System.out.println("Token generated " + authkey);
		// System.out.println(tokenret.getToken(token,"9739042229"));
		// System.out.println(tokenret.getTokenizeValue(token, "973904256734"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		boolean flag = false;
		String[] str = value.toString().split(delimiter);
		StringBuilder sb = new StringBuilder();
		try {
			for (Integer li : index) {
				if (!("".equalsIgnoreCase(str[li]) || "null".equalsIgnoreCase(str[li])
						|| " ".equalsIgnoreCase(str[li]))) {
					// System.out.println(" token "+token +" str[li]
					// "+str[li]+"prefix "+prefix +" postfix ");
					String detokenizedval = tokenret.getOriginalValue(authkey, str[li]);
					// System.out.println("tokenizedval "+tokenizedval);
					str[li] = detokenizedval;
					// System.out.println("value after tokenization "+str[li]);
				}
			}
		} catch (Exception e) {
			flag = true;
			// System.out.println("Inside catch");
			// System.out.println("excep message--------");
			// e.printStackTrace();
			text.set(value.toString() + "," + e.getMessage().toString());
			//mos.write(NullWritable.get(), text, "Quarantined/" + filename);
			

		}

		if (!flag) {
			for (String st : str) {
				sb.append(st).append(delimiter);
			}
			// System.out.println(" final value "+sb.toString());
			text.set(sb.toString().replaceAll(delimiter + "$", ""));
			mos.write(NullWritable.get(), text, "cleansed/" + filename);
		}else
		mos.write(NullWritable.get(), text, "quarantined/" + filename);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

}