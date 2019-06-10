import java.util.LinkedList;
import java.util.Stack;

/*
 * SchemaBuilder�ඨ��Schema������
 * SchemaBuilder��ʵ������transform����������JsonFactory.JsonObject���󣬲����ö����schema
 */
public class SchemaBuilder {
	static StringBuilder strSchema;
	static JsonFactory jsonFactory;

	public void transform(JsonFactory.JsonObject jsonObj){
		strSchema = new StringBuilder();
		strSchema.append("message pair{\n");
		object2Schema(jsonObj, strSchema);
		strSchema.append("}");
	}
	
	/*
	 * array2Schema�������ݴ����JsonArr����jsonArr����������Ӧ�ļ�ֵstrName������������ֵ�Ե�schema
	 * �������Ԫ���ϵ�������ȡԪ�ص�����
	 * �������������Ƕ�������飬����ֱ�ӽ�Ƕ�����ڵ�������ԣ���Ϊ���repeated��һ��repeated������Ч����ͬ
	 * ֻҪ�õ���������ĳһԪ�ص�����(���Ͳ�ΪNULL)���Ϳ��Խ���ѭ������Ϊ������Ԫ�ص�������ͬ��
	 */
	private void array2Schema(JsonFactory.JsonArray jsonArr, StringBuilder strSchema, String strName){
		for(Object obj : jsonArr.liMem){
			if(obj instanceof JsonFactory.JsonObject){
				JsonFactory.JsonObject jsonObj = (JsonFactory.JsonObject)obj;
				strSchema.append("group " + strName + "{\n");
				object2Schema(jsonObj, strSchema);
				strSchema.append("}\n");
			}
			else if(obj instanceof JsonFactory.JsonArray){
				JsonFactory.JsonArray jsonArray = (JsonFactory.JsonArray)obj;
				array2Schema(jsonArray, strSchema, strName);
			}
			else{
				if(obj == null){
					continue;
				}
				else{
					if(obj instanceof Long){
						strSchema.append("int32 " + strName + ";\n");
					}
					else if(obj instanceof Double){
						strSchema.append("float " + strName + ";\n");
					}
					else if(obj instanceof String){
						strSchema.append("binary " + strName + "(UTF8);\n");
					}
					else if(obj instanceof Boolean){
						strSchema.append("boolean " + strName + ";\n");
					}
				}
			}
			if(obj != null){
				return;
			}
		}
	}
	
	/*
	 * object2Schema�������ݴ����json����jsonObj��������Ӧ��schema
	 * ���jsonEle��optional�ģ����Ҵ�jsonEle����ΪNULL����ͨ������ͬ�ȼ�ֵ���ҵ���������
	 * ���jsonEle����optinal�ģ���������ΪNULL��ֱ�Ӻ������Ĵ��ڣ���Ϊ�޷�ͨ������ͬ�ȼ�ֵ��ȷ����������
	 */
	private void object2Schema(JsonFactory.JsonObject jsonObj, StringBuilder strSchema){
		for(JsonFactory.JsonElement jsonEle : jsonObj.liMem){

			String strType;
			if(jsonFactory.queryOptional(jsonEle)){
				if(jsonEle.strType.equals("NULL")){
					jsonEle = jsonFactory.getFirstPeerJsonElement(jsonEle);
					strType = jsonEle.strType;
					if(strType.equals("repeated")){
						strSchema.append("repeated ");
					}
					else{
						strSchema.append("optional ");
					}
				}
				else if(jsonEle.strType.equals("repeated")){
					strSchema.append("repeated ");
					strType = jsonEle.strType;
				}
				else{
					strSchema.append("optional ");
					strType = jsonEle.strType;
				}
			}
			else{
				//��jsonEle����ĳһ������ĺ����ֵ�ԣ�����������ΪNULL��������
				if(jsonEle.strType.equals("NULL")){
					continue;
				}
				else{
					strSchema.append("required ");
				}
				strType = jsonEle.strType;
			}

			switch (strType) {
			case "group":
				strSchema.append("group " + jsonEle.liPath.getLast() + "{\n");
				object2Schema((JsonFactory.JsonObject)jsonEle.objVal, strSchema);
				strSchema.append("}\n");
				break;
			case "repeated":
				array2Schema((JsonFactory.JsonArray)jsonEle.objVal, strSchema, (String)jsonEle.liPath.getLast());
				break;
			case "INT32":
				strSchema.append("int32 " + jsonEle.liPath.getLast() + ";\n");
				break;
			case "FLOAT":
				strSchema.append("float " + jsonEle.liPath.getLast() + ";\n");
				break;
			case "BINARY":
				strSchema.append("binary " + jsonEle.liPath.getLast() + "(UTF8);\n");
				break;
			case "BOOLEAN":
				strSchema.append("boolean " + jsonEle.liPath.getLast() + ";\n");
				break;
			default:
				break;
			}
		}
	}
	
	
	
	/*
	 * ȡ�ô����ֵ���ַ���strVal�����ͣ���ʱ���뱣֤strVal�Ŀ�ͷ�ͽ�β�������κο��ַ�
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
	 * ����ֵ���ַ���strPair����ð�ŷָ����β�������κο��ַ��ļ�ֵ�ַ�����ֵ�ַ���
	 * ���ش洢�������ַ���������
	 */
	static LinkedList<String> toKeyValPair(String strPair){
		LinkedList<String> liPair = new LinkedList<String>();   

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
	 * ���ݴ���ı߽��ַ�cbrace����ԭʼ�ַ���strOri�����ն��Ż���Ϊ��ֵ��token
	 */
	static LinkedList<String> tokenize(String strOri, char[] cbrace){
		// truncate the prefix until '"' and suffix invalid letters and append a ',' to it
		strOri = strOri.substring(strOri.indexOf(cbrace[0]) + 1, strOri.lastIndexOf(cbrace[1])) + ',';
		strOri = strOri.replaceFirst("\\s*", "");
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
					if(!strToken.isEmpty()){
						liToken.add(strToken);
					}
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
	 * ȥ���ַ�����β�Ŀ��ַ�
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
