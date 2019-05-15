import java.util.List;
import java.util.LinkedList;
import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;

public class Transformer {
	static JsonObject jsonObj;
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	static String transform(String strJson){
		String strSchema;
		return strSchema;
	}
	
	static void initialLists(LinkedList<Integer> liBranchLoc, LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt
			, LinkedList<Object> liPath){
		for(int nInd = 0; nInd != liPath.size() - 1; ++nInd){
			if(liPath.get(nInd + 1) instanceof Integer){
				liBranchLoc.push(nInd + 1);
				LinkedList<Object> liPathTmp = new LinkedList<>();
				for(int nInd1 = 0; nInd1 != nInd + 1; ++nInd1){
					liPathTmp.add(liPath.get(nInd1));
				}
				Object jsonEle1 = null;
				getJsonVal(liPathTmp, jsonEle1);
				liBranchSize.push(((JsonArray)jsonEle1).liMem.size());
				liBranchCnt.push(0);
			}
		}	
	}
	static boolean incrementBranch(LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt, 
			int nlevel){
		boolean boverflow = true;
		for(int nliInd = 0; nliInd != liBranchCnt.size(); ++nliInd){
			if(liBranchCnt.get(nliInd) < liBranchSize.get(nliInd) - 1){
				boverflow = false;
			}
			if(boverflow){
				return false;
			}
		}
		
		int num = liBranchCnt.get(nlevel) + 1;
		if(num == liBranchSize.get(nlevel)){
			liBranchCnt.set(nlevel, 0);
			incrementBranch(liBranchSize, liBranchCnt, nlevel - 1);
		}
		else{
			liBranchCnt.set(nlevel, num);
		}
		return true;
	}
	static boolean queryOptional(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>();
		
		initialLists(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		
		int nBranchNum = liBranchCnt.size();
		if(nBranchNum == 0){
			return false;
		}
		else{
			do{
				if(!incrementBranch(liBranchSize, liBranchCnt, nBranchNum)){
					return false;
				}
				else{
					for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
						liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
					}
					Object jsonVal = null;
					if(getJsonVal(liPath, jsonVal)){
						if(jsonVal == null){
							return true;
						}
					}
				}
			}
			while(true);
		}
		
	}
	static boolean getJsonVal(LinkedList<Object> liPath, Object jsonEle){
		Object objCur = jsonObj;

		for(Object objLoc : liPath){
			if(objLoc instanceof String){
				if(objCur instanceof JsonObject){
					String strLoc = (String)objLoc;
					JsonObject jsonObj = (JsonObject)objCur;
					if(!jsonObj.getVal(strLoc, objCur)){
						return false;
					}
				}
				else{
					return false;
				}
			}
			else if(objLoc instanceof Integer){
				if(objCur instanceof JsonArray){
					Integer nLoc = (Integer)objLoc;
					JsonArray jsonArr = (JsonArray)objCur;
					if(!jsonArr.getVal(nLoc, objCur)){
						return false;
					}
				}
				else{
					return false;
				}
			}
		}
		
		jsonEle = objCur;
		return true;
	}

	/*
	 * Encode strOri into schema. If object is a top-level object then ilayer equal to 0
	 * if object is a nested object then ilayer equal to a positive
	 */
	static void Object2Schema(JsonObject jsonObj, StringBuilder strSchema){
		strSchema.append("{\n");
		for(JsonElement pair : jsonObj.liMem){
		}
		strSchema.append("}");
	}
	
	private static JsonArray buildArr(String strArr){
		JsonArray jsonArray = new JsonArray();

		char[] cbrace = new char[]{'[', ']'};
		LinkedList<String> liToken = tokenize(strArr, cbrace); //Tokenize a array closing by a couple of square brackets
		int nliToken = 0;     //nliToken counts the index of current element
		for(String str : liToken){
			stkPath.push(nliToken++);

			str = tailorStr(str);
			String strType = getType(str);
			switch (strType) {
			case "group":
				jsonArray.addValue(buildObj(str));
				break;
			case "repeated":
				jsonArray.addValue(buildArr(str));
				break;
			case "INT32":
				jsonArray.addValue(Integer.parseInt(str));
				break;
			case "FLOAT":
				jsonArray.addValue(Float.parseFloat(str));
				break;
			case "BINARY":
				jsonArray.addValue(str + "(UTF8)");
				break;
			case "BOOLEAN":
				jsonArray.addValue(Boolean.parseBoolean(str));
				break;
			case "NULL":
				jsonArray.addValue(null);
				break;
			default:
				break;
			}
			
			stkPath.pop();
		}
		return jsonArray;
	}
	public static JsonObject buildObj(String strJson){
		JsonObject jsonObject = new JsonObject();

		char[] cbrace = new char[]{'{', '}'};
		List<String> liToken = tokenize(strJson, cbrace);
		for(String str : liToken){
			List<String> pair = toKeyValPair(str);
			stkPath.push(pair.get(0));

			JsonElement jsonElement;
			String strType = getType(pair.get(1));   //return the type of value
			switch(strType){
			case "group":
				jsonElement = new JsonElement(buildObj(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "repeated":
				jsonElement = new JsonElement(buildArr(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "INT32":
				jsonElement = new JsonElement(Integer.parseInt(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "FLOAT":
				jsonElement = new JsonElement(Float.parseFloat(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "BINARY":
				jsonElement = new JsonElement(pair.get(1), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "BOOLEAN":
				jsonElement = new JsonElement(Boolean.parseBoolean(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "NULL":
				jsonElement = new JsonElement(null, strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			default:
				break;
			}
			
			stkPath.pop();
		}
		return jsonObject;
	}
	
	/*
	 * Get the type of value string in a key-value pair. Argument strVal contains no null-letter.
	 * Type contains following "group, repeated, BINARY, FLOAT, INT32, BOOLEAN, NULL"
	 */
	private static String getType(String strVal){
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
	private static List<String> toKeyValPair(String strPair){
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
	public static LinkedList<String> tokenize(String strOri, char[] cbrace){
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
	private static String tailorStr(String oriStr){
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
