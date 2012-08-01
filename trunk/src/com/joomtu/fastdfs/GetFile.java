package com.joomtu.fastdfs;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import org.csource.common.MyException;
import org.csource.fastdfs.ProtoCommon;

public class GetFile {

	/**
	 * @param args
	 * @throws MyException
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.out.println(ProtoCommon.getToken(
					"toMRCk_W9IcIAAAAAAAiVGyQkowAAAAMgMn-YwAACJs799.jpg",
					1333180895, "Fastdfs0123456789"));
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}

}
