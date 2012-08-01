/** Copyright (C) 2012 Happy Fish / YuQing
 *  My FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package com.joomtu.fastdfs;

import java.util.Hashtable;

import org.csource.fastdht.ClientGlobal;
import org.csource.fastdht.FastDHTClient;
import org.csource.fastdht.KeyInfo;
import org.csource.fastdht.ObjectInfo;

/**
 * My FastDFS test
 * 
 * @author Happy Fish / YuQing
 * @version Version 1.00
 */
public class MyFDFSClientTest {

	public MyFastDFSClient fdfsClient;
	public FastDHTClient fdhtClient;
	public final String fdhtNamespace = "FastDFS";
	public final String fdfsConfigFilename = "/usr/local/application/fastdfs-3.08/etc/client.conf";
	public final String fdhtConfigFilename = "/usr/local/application/fastdht-1.20/etc/fdht_client1.conf";

	public MyFDFSClientTest() {
		try {
			MyFastDFSClient.init(fdfsConfigFilename, fdhtConfigFilename);
			fdfsClient = new MyFastDFSClient(fdhtNamespace);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		MyFDFSClientTest test = new MyFDFSClientTest();
		test.upload();
		//System.out.println(test.getFileNameMyFileId("3"));
		test.getFileNameByKey();

	}

	public void getFileNameByKey() {
		try {
			ClientGlobal.init(fdhtConfigFilename);
			KeyInfo keyInfo;
			@SuppressWarnings("rawtypes")
			Hashtable stats;

			System.out.println("network_timeout="
					+ ClientGlobal.g_network_timeout + "ms");
			System.out.println("charset=" + ClientGlobal.g_charset);

			ClientGlobal.g_server_group.print();

			fdhtClient = new FastDHTClient(false);
			try {
				keyInfo = new KeyInfo("bbs", "test", "username");

				System.out.println("set: "
						+ fdhtClient.set(keyInfo, "12345678901234"));
				System.out.println("get: " + fdhtClient.get(keyInfo));
				System.out.println("inc: " + fdhtClient.inc(keyInfo, 100));
				// System.out.println("delete: " + client.delete(keyInfo));
				System.out.println("get 3.jpg: " + fdhtClient.get(new KeyInfo(fdhtNamespace, "3", "fdfs_fid")));
				for (int i = 0; i < ClientGlobal.g_server_group.getGroupCount(); i++) {
					stats = fdhtClient.stat(i);
					if (stats == null) {
						continue;
					}

					System.out.println("server="
							+ ClientGlobal.g_server_group.getServers()[i]
									.getAddress().getAddress().getHostAddress()
							+ ":"
							+ ClientGlobal.g_server_group.getServers()[i]
									.getAddress().getPort());
					System.out.println(stats.toString());
				}

				ObjectInfo objInfo = new ObjectInfo(keyInfo.getNamespace(),
						keyInfo.getObjectId());
				System.out.println("sub keys: "
						+ java.util.Arrays.toString(fdhtClient
								.getSubKeys(objInfo)));
			} finally {
				fdhtClient.close();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public String getFileNameMyFileId(String myFileId) {
		try {
			return fdfsClient.get_fdfs_file_id(myFileId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "xx";
		}
	}

	public void upload() {

		try {

			final String my_file_id = "3";
			final String local_filename = "/home/mohadoop/图片/3.jpg";
			final String file_ext_name = "";
			int result;
			if ((result = fdfsClient.upload_file(my_file_id, local_filename,
					file_ext_name)) != 0) {
				System.err.println("upload_file fail, errno: " + result);
				return;
			}

			System.out.println("fdfs_file_id: "
					+ fdfsClient.get_fdfs_file_id(my_file_id));

			if ((result = fdfsClient.download_file(my_file_id,
					"/home/mohadoop/图片/3x.jpg")) != 0) {
				System.err.println("download_file fail, errno: " + result);
				return;
			}
			// ./fdht_get ../etc/fdht_client.conf FastDFS:3 fdfs_fid
			//System.out.println("delete_file result: "
					//+ fdfsClient.delete_file(my_file_id));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}