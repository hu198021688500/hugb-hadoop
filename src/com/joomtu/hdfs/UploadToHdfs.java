package com.joomtu.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadToHdfs {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path src = new Path("/home/mohadoop/upload");
		Path dst = new Path("hdfs://host185.freebsd.hu:9000/user/mohadoop/testdata/upload");
		hdfs.copyFromLocalFile(false, src, dst);
	}

}
