package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * 用于结果汇总
 * @author jamesxia
 *
 */
public class ResultsSummary {
	public static void main(String[] args) throws Exception {
		PrintWriter pw = new PrintWriter(new FileWriter("/Users/jamesxia/Documents/NutStore/大四下/毕设/计软毕设/论文/all-result.txt"), true);
		File dir = new File("/Users/jamesxia/Documents/NutStore/大四下/毕设/计软毕设/论文/实验结果txt");
		
		pw.println("推荐器,数据集,正则化参数,特征维度,预测RMSE");
		if(dir.isDirectory()){
			File[] listFiles = dir.listFiles();
			
			//读取每一个结果文件
			for (File file : listFiles) {
				String dataset = "";
				if(file.getName().contains("ml-1m")){
					dataset = "ml-1m";
				} else if(file.getName().contains("10M100K")) {
					dataset = "10M100K";
				} else if(file.getName().contains("ml-20m")){
					dataset = "ml-20m";
				} else if(file.getName().contains("ml-latest")){
					dataset = "ml-latest";
				} else if(file.getName().contains("neflix")){
					dataset = "neflix";
				}
				//读出每一条记录
				BufferedReader bufr = new BufferedReader(new FileReader(file));
				
				String line = null;
				while((line = bufr.readLine()) != null){
					String[] strings = line.split(",");
					
					if(strings.length == 4){//mahout结果
						pw.println("MahoutSVDRecommender," + 
									dataset + "," +
									strings[1].substring(strings[1].indexOf('：')+1) + "," +
									strings[2].substring(strings[2].indexOf('：')+1) + "," + 
									strings[3].substring(strings[3].indexOf('：')+1));
					} else if(strings.length == 7) {//RSVD，BiasSVD
						pw.println(strings[0].substring(strings[0].indexOf('：')+1) + "," + 
								dataset + "," +
								strings[2].substring(strings[2].indexOf('：')+1) + "," +
								strings[3].substring(strings[3].indexOf('：')+1) + "," + 
								strings[6].substring(strings[6].indexOf('：')+1));
					} else if(strings.length == 8) {//MyRecommender
						pw.println(strings[0].substring(strings[0].indexOf('：')+1) + "-alpha=" + strings[6].substring(strings[6].indexOf('：')+1) + "," + 
								dataset + "," +
								strings[2].substring(strings[2].indexOf('：')+1) + "," +
								strings[3].substring(strings[3].indexOf('：')+1) + "," + 
								strings[7].substring(strings[7].indexOf('：')+1));
					}
					
				}
				if(bufr != null)
					bufr.close();
			}
			System.out.println("结果整合成功");
			if (pw != null) {
				pw.close();
			}
		}
		
	}
}
