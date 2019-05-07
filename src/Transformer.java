import java.util.List;
import java.util.LinkedList;
import java.util.Stack;

public class Transformer {
	/*
	 * encode strJson into schema. If object is a top-level object then ilayer equal to 0
	 * if object is a nested object then ilayer equal to a positive
	 */
	private static String INT322Schema(String strINT32, String strName){
	}
	private static String Obj2Schema(String strJson, String strName, int ilayer){
		String strSchema;
		if(ilayer == 0){
			strSchema = "message pair {\\n";
		}
		else{
			strSchema = "group" + strName + "{\\n";
		}

		List<String> liToken = Tokenize(strJson);
		for(String str : liToken){
			List<String> pair = ToKeyValPair(str);
			String strType = GetType(pair.get(1));
			switch(strType){
			case "group":
				strSchema += Obj2Schema(pair.get(1), pair.get(0), 1);
				break;
			case "repeated":
			}
		}

		if(ilayer == 0){
			strSchema += "}\\n";
		}
		else{
			strSchema += "}";
		}
		return strSchema;
	}
	private static String GetType(String strVal){
		char cfst = strVal.charAt(0);
		switch(cfst){
		case '{':
			return "group";
		case '[':
			return "repeated";
		case '\"':
			return "BINARY";
		default:
			if(strVal.contains(".")){
				return "FLOAT";
			}
			else{
				return "INT32";
			}
		}
	}
	/*
	 * we assume every key-value pair is followed by a comma immediately
	 * After ToKeyValPair function the string in returned pair will contain no null-letter
	 */
	private static List<String> ToKeyValPair(String strPair){
		List<String> liPair = new LinkedList<String>();
		String strKey, strVal;
		Stack<Character> stk = new Stack<>();
		int nLen = strPair.length();
		for(int nStp = 0; nStp != nLen; ++nStp){
			switch(strPair.charAt(nStp)){
			case '"':
				if(stk.empty()){
					stk.push('"');
				}
				else{
					if(strPair.charAt(nStp - 1) != '\\'){
						stk.pop();
						if(stk.empty()){
							strKey = strPair.substring(0, nStp + 1);
						}
					}
				}
				break;
			case ':':
				if(stk.empty()){
					strVal = strPair.substring(nStp + 1);
					strVal = strVal.replaceFirst("\\s*", "");
					liPair.add(strVal);
				}
				break;
			}
		}
		return liPair;
	}
	private static List<String> Tokenize(String strJson){
		// truncate the prefix until '"' and suffix invalid letters and append a ',' to it
		strJson = strJson.substring(strJson.indexOf("\""), strJson.lastIndexOf("}")) + ',';
		List<String> liToken = new LinkedList<String>();
		String strToken;

		/*
		 * split out key-value pair around ',' and it could introduce some invalid letter 
		 * sequence issue. We will deal with these problem in ToKeyValPair
		 */
		int nLen = strJson.length();
		Stack<Character> stk = new Stack<>();
		for(int nStr = 0, nStp = 0; nStp != nLen; ++nStp){
			switch(strJson.charAt(nStp)){
			case '"':
				if(stk.empty()){
						stk.push('"');
				}
				else{
					if(strJson.charAt(nStp - 1) != '\\'){
						stk.pop();
					}
				}
				break;
			case ',':
				if(stk.empty()){
					strToken = strJson.substring(nStr, nStp);
					liToken.add(strToken);
					stk.clear();
					nStr = strJson.indexOf('"', nStp);
				}
				break;
			}
		}
		
		return liToken;
	}
}
