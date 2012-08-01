package com.joomtu.mapred;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogAnalysiser {

	public static class MapClass extends
			Mapper<Object, Text, Text, LongWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line == null || line.equals(""))
				return;
			String[] words = line.split(",");
			if (words == null || words.length < 8)
				return;
			String appid = words[1];
			String apiName = words[2];
			LongWritable recbytes = new LongWritable(Long.parseLong(words[7]));
			Text word = new Text();
			word.set(new StringBuffer("flow::").append(appid).append("::")
					.append(apiName).toString());
			context.write(word, recbytes); // 输出流量的统计结果，通过flow::作为前缀来标示。
			word.clear();
			word.set(new StringBuffer("count::").append(appid).append("::")
					.append(apiName).toString());
			context.write(word, new LongWritable(1));// 输出次数的统计结果，通过count::作为前缀来标示
		}
	}

	public static class PartitionerClass extends
			Partitioner<Text, LongWritable> {
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			if (numPartitions >= 2)// Reduce 个数，判断流量还是次数的统计分配到不同的Reduce
				if (key.toString().startsWith("flow::"))
					return 0;
				else
					return 1;
			else
				return 0;
		}
	}

	public static class ReduceClass extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			//
			Text newkey = new Text();
			newkey.set(key.toString().substring(
					key.toString().indexOf("::") + 2));
			LongWritable result = new LongWritable();
			long sum = 0;
			int counter = 0;
			for (LongWritable val : values) {// 累加同一个key的统计结果
				sum += val.get();
				// 担心处理太久，JobTracker长时间没有收到报告会认为TaskTracker已经失效，因此定时报告一下
				counter = counter + 1;
				if (counter == 1000) {
					counter = 0;
				}
			}
			result.set(sum);
			context.write(newkey, result);// 输出最后的汇总结果
		}
	}

	public static class CombinerClass extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			LongWritable result = new LongWritable();
			long sum = 0;
			for (LongWritable val : values) {// 累加同一个key的统计结果
				sum += val.get();
			}
			result.set(sum);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if (args == null || args.length < 2) {
			System.out.println("need inputpath and outputpath");
			return;
		}
		String inputpath = args[0];
		String outputpath = args[1];
		// 文件名
		String shortin = args[0];
		String shortout = args[1];
		if (shortin.indexOf(File.separator) >= 0) {
			shortin = shortin.substring(shortin.lastIndexOf(File.separator));
		}
		if (shortout.indexOf(File.separator) >= 0) {
			shortout = shortout.substring(shortout.lastIndexOf(File.separator));
		}
		//
		SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");
		shortout = new StringBuffer(shortout).append("-")
				.append(formater.format(new Date())).toString();
		if (!shortin.startsWith("/")) {
			shortin = "/" + shortin;
		}
		if (!shortout.startsWith("/")) {
			shortout = "/" + shortout;
		}
		shortin = "/user/root" + shortin;
		shortout = "/user/root" + shortout;
		File inputdir = new File(inputpath);
		File outputdir = new File(outputpath);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.out.println("inputpath not exist or isn't dir!");
			return;
		}
		if (!outputdir.exists()) {
			new File(outputpath).mkdirs();
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "analysis job");
		job.setJarByClass(LogAnalysiser.class);
		FileSystem fileSys = FileSystem.get(conf);
		fileSys.copyFromLocalFile(new Path(inputpath), new Path(shortin));// 将本地文件系统的文件拷贝到HDFS中
		job.setJobName("analysisjob");
		job.setOutputKeyClass(Text.class);// 输出的key类型，在OutputFormat会检查
		job.setOutputValueClass(LongWritable.class); // 输出的value类型，在OutputFormat会检查
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombinerClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(job, shortin); // hdfs中的输入路径
		FileOutputFormat.setOutputPath(job, new Path(shortout));// hdfs中输出路径
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		// 删除输入和输出的临时文件
		fileSys.copyToLocalFile(new Path(shortout), new Path(outputpath));
		fileSys.delete(new Path(shortin), true);
		fileSys.delete(new Path(shortout), true);
	}

}
