package pythonscripts.Source.Misc.taasTokenizer.src.com.bt.taasTokenizer.numberTokenizer;

public class Formatter {

	public String getFormattedNumber(String prefix, String changeableNum, String postfix, String fieldType) {
		
		String formattedNum = "";
		String trimmedNum = "";
		int startFrom = Integer.parseInt(prefix);
		int endAt = Integer.parseInt(postfix);
		
		if("T".equalsIgnoreCase(fieldType)) {
			trimmedNum = trimString(changeableNum);
		} else {
			trimmedNum = changeableNum;
		}
		
		String extractedNumber = trimmedNum.substring(startFrom, trimmedNum.length() - endAt);
		String tokenizedNum = new TokenGen().getTokenizedNumber(extractedNumber, fieldType);
		
		formattedNum = trimmedNum.substring(0, startFrom) + tokenizedNum 
						+ trimmedNum.substring(trimmedNum.length() - endAt);
		
		return formattedNum;
	}
	
	public String trimString(String oldString) {
		
		String newString = oldString;
		
		if(oldString.contains(" ")) {
			newString = oldString.replace(" ", "");
		}
		if(oldString.contains("-")) {
			newString = newString.replace("-", "");
		}
		if(oldString.contains("(")) {
			newString = newString.replace("(", "");
		}
		if(oldString.contains(")")) {
			newString = newString.replace(")", "");
		}		
		
		return newString;
	}
}
