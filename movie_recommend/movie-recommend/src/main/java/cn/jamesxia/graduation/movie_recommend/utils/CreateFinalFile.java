package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;

public class CreateFinalFile {
	public static void main(String[] args) throws Exception {
		File dir = new File("/Users/jamesxia/Downloads/download/training_set");
		File[] files = dir.listFiles();
		
		PrintWriter pw = new PrintWriter(new FileOutputStream("/Users/jamesxia/Downloads/download/neflix-all-notime.csv"), true);
		int count = 1;
		for (File file : files) {
			BufferedReader bufr = new BufferedReader(new FileReader(file));
			String line = bufr.readLine();//先读掉第一行
			while((line = bufr.readLine()) != null) {
				String[] strings = line.split(",");
//				pw.println(strings[0] + "," + count + "," + strings[1] + "," + strings[2]);
				pw.println(strings[0] + "," + count + "," + strings[1]);
			}
			count++;
			bufr.close();
//			if(count > 2)
//				break;
		}
		pw.close();
	}
}
