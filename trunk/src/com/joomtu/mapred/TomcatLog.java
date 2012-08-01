package com.joomtu.mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TomcatLog {
	static class TomcatMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private static Pattern pattern = Pattern
				.compile("([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),(.*)");

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println(line);
			Matcher m = pattern.matcher(line);
			if (m.matches()) {
				String agent = m.group(9).toLowerCase();
				if (agent.contains("chrome")) {
					agent = "chrome";
				} else if (agent.contains("safari")) {
					agent = "safari";
				} else if (agent.contains("firefox")) {
					agent = "firefox";
				} else {
					agent = "other";
				}
				Text t = new Text(agent);
				context.write(t, one);
			}
		}
	}

	static class TomcatReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(
				Text key,
				java.lang.Iterable<IntWritable> value,
				org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable v : value) {
				count = count + v.get();
			}
			context.write(key, new IntWritable(count));
		};
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("参数个数不对");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(TomcatLog.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TomcatMapper.class);
		job.setReducerClass(TomcatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
