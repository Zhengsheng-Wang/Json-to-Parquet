import java.util.List;
import java.util.LinkedList;
import java.util.Stack;

public class SchemaBuilder {
	static StringBuilder strSchema;

	public void transform(JsonFactory.JsonObject jsonObj){
		strSchema = new StringBuilder();
		strSchema.append("message pair{\n");
		Object2Schema(jsonObj, strSchema);
		strSchema.append("}");
	}
	/*
	 * Encode strOri into schema. If object is a top-level object then ilayer equal to 0
	 * if object is a nested object then ilayer equal to a positive
	 */
	static void Object2Schema(JsonFactory.JsonObject jsonObj, StringBuilder strSchema){
		strSchema.append("{\n");
		for(JsonElement pair : jsonObj.liMem){
		}
		strSchema.append("}");
	}
	
	
	
	/*
	 * Get the type of value string in a key-value pair. Argument strVal contains no null-letter.
	 * Type contains following "group, repeated, BINARY, FLOAT, INT32, BOOLEAN, NULL"
	 */
	static String getType(String strVal){
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
			else if(strVal.equals("true") || strVal.equals("True") || strVal.equals("TRUE") 
					|| strVal.equals("false") || strVal.equals("False") || strVal.equals("FALSE")){
				return "BOOLEAN";
			}
			else if(strVal.equals("null") || strVal.equals("Null") || strVal.equals("NULL")){
				return "NULL";
			}
			else{
				return "INT32";
			}
		}
	}

	/*
	 * Split key-value pair strPair into a list of two items by ':', which are key and value respectively.
	 * After this function the string in returned pair will contain no null-letter.
	 * We store a key-value pair in a two-item list.
	 */
	static List<String> toKeyValPair(String strPair){
		List<String> liPair = new LinkedList<String>();   

		strPair = tailorStr(strPair);
		int nStp = 0;
		String strKey, strVal;

		Stack<Character> stk = new Stack<>();
		boolean iflag = false;
		while(!iflag){
			switch(strPair.charAt(nStp)){
			case '"':
				if(stk.empty()){
					stk.push('"');
				}
				else{
					//This '"' is a close quote
					if(strPair.charAt(nStp - 1) != '\\' && stk.get(stk.size() - 1) == '"'){
						stk.pop();
					}
				}
				break;
			case ':':
				//This ':' is the spliter between key and value
				if(stk.empty()){
					iflag = true;
				}
				break;
			}
			++nStp;  //When nStp is at the position of ':', it will still be plus 1
		}
		--nStp; //Move nStp to the position of ':'
		strKey = strPair.substring(0, nStp);  //strKey contains open quote, close quote, tail null-letters
		strKey = tailorStr(strKey);
		strKey = strKey.substring(1, strKey.length() - 1);  //Remove the quotes 

		strVal = strPair.substring(nStp + 1);
		strVal = tailorStr(strVal);
		
		liPair.add(strKey);
		liPair.add(strVal);
		
		return liPair;
	}

	/*
	 * Tokenize string strJson using splitting char ',', 
	 * depending on the open and close brace chars we set.
	 */
	static LinkedList<String> tokenize(String strOri, char[] cbrace){
		// truncate the prefix until '"' and suffix invalid letters and append a ',' to it
		strOri = strOri.substring(strOri.indexOf(cbrace[0]) + 1, strOri.lastIndexOf(cbrace[1])) + ',';
		LinkedList<String> liToken = new LinkedList<String>();
		String strToken;

		/*
		 * Split out key-value pair around ',' and it could introduce some null-letter 
		 * sequence issue. We will deal with these problem in toKeyValPair
		 */
		int nLen = strOri.length();
		Stack<Character> stk = new Stack<>();
		for(int nStr = 0, nStp = 0; nStp != nLen; ++nStp){
			switch(strOri.charAt(nStp)){
			case '"':
				//Any letter contained in a pair of quotes("") will be considered belong a string
				if(stk.empty()){
						stk.push('"');
				}
				else{
					if(strOri.charAt(nStp - 1) != '\\' && stk.get(stk.size() - 1) == '"'){
						stk.pop();
					}
				}
				break;
			case '[':
				//If before we encounter '[' there is a '"' in stack, then we miss that '['
				//If not, that '[' marks the begin of an array
				if(stk.empty() || stk.get(stk.size() - 1) != '"'){
					stk.push('[');
				}
				break;
			case ']':
				//If stack doesn't contain a '"', it means this ']' is not belong to a string
				if(stk.get(stk.size() - 1) != '"'){
					stk.pop();
				}
				break;
			case '{':
				if(stk.empty() || stk.get(stk.size() - 1) != '"'){
					stk.push('{');
				}
				break;
			case '}':
				if(stk.get(stk.size() - 1) != '"'){
					stk.pop();
				}
				break;
			case ',':
				//If there is any char in stack, it means this ',' doesn't split this object
				if(stk.empty()){
					strToken = strOri.substring(nStr, nStp);
					liToken.add(strToken);
					nStr = nStp + 1;   //Move nStr to nStp + 1, if this comma is the last comma, 
									   //nStr == nLen
					nStp = nStr == nLen ? nLen - 1 : nStp; //If nStp is nLen - 1, in the next loop
														   //nStp = nLen, then step out the loop
				}
				break;
			default:
				break;
			}
		}
		
		return liToken;
	}

	/*
	 * remove the head null-letters and tail null-letters of oriStr
	 */
	static String tailorStr(String oriStr){
		String resStr = "";
		oriStr = oriStr.replaceFirst("\\s*", "");

		//oriStr only contains null-letters
		if(oriStr.length() == 0){
			return resStr;
		}

		int nLen = oriStr.length();
		int nStp = nLen - 1;
		while(oriStr.charAt(nStp) == ' ' || oriStr.charAt(nStp) == '	'){
			--nStp;
		}
		nLen = nStp + 1;
		resStr = oriStr.substring(0, nLen);  //earse tail null-letters

		return resStr;
	}
}
