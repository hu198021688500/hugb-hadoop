package com.joomtu.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String line = "221.237.165.94 - - [03/Aug/2011:16:56:58 +0800] \"GET / HTTP/1.1\" 200 7886 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:5.0) Gecko/20100101 Firefox/5.0\"";
		Pattern pattern = Pattern
				.compile("([\\d\\.]*) - (.*) \\[(.*)\\] \"(.*?)\" (\\d*) (\\d*) \"(.*)\" \"(.*)\"(.*)", Pattern.CANON_EQ);
		
		//"(.)* - (.)* [(.*)] \"(.*)\" (.)* (.)* \"(.*)\" \"(.*)\"(.)*"
		
		Matcher m = pattern.matcher(line);
		if (m.matches()) {
			System.out.println("Maches");
			for (int i = 0; i <= m.groupCount(); i++) {
				System.out.println(i);
				System.out.println(m.group(i));
			}
		} else {
			System.out.println("Not maches");
		}
	}

}
