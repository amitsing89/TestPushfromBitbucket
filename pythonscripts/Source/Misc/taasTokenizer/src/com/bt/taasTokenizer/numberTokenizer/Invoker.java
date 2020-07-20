package pythonscripts.Source.Misc.taasTokenizer.src.com.bt.taasTokenizer.numberTokenizer;

public class Invoker {

	public static void main(String[] args) {
		
		String fixedPrefix = args[0]; //ignore the beginning d digits
		String originalNumber = args[1]; //actual number to be tokenized
		String fixedPostfix = args[2]; // preserve the last digits 
		String fieldType = args[3]; // type of input - only for telephone
		
		Formatter formatter = new Formatter();
		String tokenizedNum = formatter.getFormattedNumber(fixedPrefix, originalNumber, fixedPostfix, fieldType);
		
		System.out.println("Old String: " + originalNumber);
		System.out.println("New Tokenized String: " + tokenizedNum);
	}
}
