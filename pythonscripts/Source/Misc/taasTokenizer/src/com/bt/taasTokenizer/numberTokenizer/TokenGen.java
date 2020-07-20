package pythonscripts.Source.Misc.taasTokenizer.src.com.bt.taasTokenizer.numberTokenizer;

import java.util.Random;

public class TokenGen {

	public String getTokenizedNumber(String formattedNumber, String fieldType) {
		
		char[] origArr = formattedNumber.toCharArray();
		char[] tempArr = origArr;
		Random random = new Random();
		
		for(int i = 0; i < origArr.length; i++) {			
			
			int n = random.nextInt(origArr.length - 1) + 0;
			
			if("T".equalsIgnoreCase(fieldType) && (i != 0 && n != 0)) {
				tempArr[i] = origArr[n];
			} else {
				tempArr[i] = origArr[n];
			}
		}
		
		return String.valueOf(tempArr);
	}
}
