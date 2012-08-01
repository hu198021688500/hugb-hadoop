package com.joomtu.hdfs;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class ReadFromHdfs {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		// hdfs://host185.freebsd.hu:9000/user/mohadoop/
		Path path = new Path(
				"hdfs://host185.freebsd.hu:9000/user/mohadoop/testdata/synthetic_control.data");

		boolean isExists = hdfs.exists(path);

		FileStatus fileStatus = hdfs.getFileStatus(path);
		long modificationTime = fileStatus.getModificationTime();

		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());
		int blkCount = blkLocations.length;
		for (int i = 0; i < blkCount; i++) {
			String[] hosts = blkLocations[i].getHosts();
			for (int j = 0; j < hosts.length; j++) {
				System.out.println(hosts[j]);
			}
		}

		System.out.println();

		DistributedFileSystem xx = (DistributedFileSystem) hdfs;
		DatanodeInfo[] dataNodeStatus = xx.getDataNodeStats();
		for (int i = 0; i < dataNodeStatus.length; i++) {
			System.out.println(dataNodeStatus[i].getHostName());
		}

		System.out.println();

		// boolean isDeleted = hdfs.delete(path, false);
		// boolean isRenamed = hdfs.rename(formPath, toPath);
		// FSDataOutputStream outputStream = hdfs.create(path);
		// outputStream.write(bugg, 0, len)

		System.out.println(isExists);

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = format.format(modificationTime);
		System.out.println(dateString);
	}

}
