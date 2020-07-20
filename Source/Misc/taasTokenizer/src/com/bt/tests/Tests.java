package com.bt.tests;

import java.util.Random;

public class Tests {

	public static void main(String[] args) {
		
		String shellCmd = "RET_DAYS_RAW_DATA=$1\n"
				+ "PATH_RAW_DATA=$2\n"
				+ "#RET_DAYS_PROCESSED_DATA=$3\n"
				+ "#PATH_PROCESSED_DATA=$4\n"
				+ "if [ -z \"$RET_DAYS_RAW_DATA\" ] && [ \"$RET_DAYS_RAW_DATA\" == \"\" ] && [ -z \"$PATH_RAW_DATA\" ] && [ \"$PATH_RAW_DATA\" == \"\" ]; then\n"
				+ "echo \"empty\"\n"
				+ "else\n"
				+ "date_threshold=$(date -d \"-$RET_DAYS_RAW_DATA days\" +'%Y/%m/%d')\n"
				+ "for i in `hadoop fs -ls -R $PATH_RAW_DATA | awk '{print $1\"|\"$6\"|\"$8}'`\n"
				+ "do \n"
				+ "IFS=\"|\"; declare -a Array=($i)\n"
				+ "DIRECTORY_DATE=${Array[1]}\n"
				+ "DIRECTORY_NAME=${Array[2]}\n"
				+ "#echo $DIRECTORY_NAME\n"
				+ "if [[ ${i:0:1} == 'd' && ${#DIRECTORY_DATE} -eq 10 ]]; then \n"
				+ "#echo $DIRECTORY_NAME; \n"
				+ "if [[ $DIRECTORY_DATE < $date_threshold ]]; then \n"
				+ "hadoop fs -rm -r $DIRECTORY_NAME\n" + "fi\n"
				+ "fi\n" + "done\n" + "fi\n";
		
		System.out.println(shellCmd);
		
		/*String number = "+919980350523";
		int prefix = 3;
		int postfix = 2;
		
		System.out.println(number.substring(prefix, number.length() - postfix));
		System.out.println(number.substring(0, prefix));
		System.out.println(number.substring(number.length() - postfix));
		
		char[] charArrays = new char[]{'1', '2', '3', 'A', 'B', 'C'};
		System.out.println(String.valueOf(charArrays));*/
		
		/*String tnum = "9980350523";
		char[] charArr = tnum.toCharArray();
		char[] tempArr = charArr;		
		Random random = new Random();
		
		for(int i = 0; i < charArr.length; i++) {			
			
			int n = random.nextInt(9) + 0;
			if(i != 0 && n != 0) {
				tempArr[i] = charArr[n];
			}			
		}
		System.out.println(tempArr);*/
		
		/*String testStr = "98765 44543";
		System.out.println(testStr.replace(" ", ""));*/
	}
}
