package com.joomtu.mapred;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NginxLog {
	
	public static class NginxMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private static Pattern pattern = Pattern
				.compile(
						"([\\d\\.]*) - (.*) \\[(.*)\\] \"(.*?)\" (\\d*) (\\d*) \"(.*)\" \"(.*)\"(.*)",
						Pattern.CASE_INSENSITIVE);
		private Text text = new Text();

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Matcher m = pattern.matcher(line);
			if (m.matches()) {
				String ip = m.group(1).toLowerCase();
				text.set(ip);
				context.write(text, one);

				String agent = m.group(8).toLowerCase();
				if (agent.contains("chrome")) {
					agent = "chrome";
				} else if (agent.contains("safari")) {
					agent = "safari";
				} else if (agent.contains("firefox")) {
					agent = "firefox";
				} else {
					agent = "other";
				}
				text.set(agent);
				//context.write(text, one);
				
				String file = m.group(4).toLowerCase();
				if (file.contains(".php")) {
					text.set(file);
					//context.write(text, one);
				} 

			}
		}
	}

	public static class NginxReducer extends
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

	public static class SortMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text values, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(values.toString());
			while (itr.hasMoreTokens()) {
				Text v = new Text(itr.nextToken());
				context.write(v, v);
			}
		}
	}

	public static class SortReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text data : values) {
				context.write(key, data);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
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

		Configuration conf = new Configuration();
		Job job = new Job(conf, "nginx access ip count");
		job.setJarByClass(NginxLog.class);

		Path tempDir = new Path("temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, tempDir);

			job.setMapperClass(NginxMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(NginxReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			if (job.waitForCompletion(true)) {
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(NginxLog.class);

				FileInputFormat.addInputPath(sortJob, tempDir);
				FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));

				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				sortJob.setOutputFormatClass(TextOutputFormat.class);

				sortJob.setMapperClass(InverseMapper.class);
				sortJob.setNumReduceTasks(1);

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);

				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(tempDir);
		}
	}

}
