import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public class JsonFactory {
	static JsonObject jsonObj;
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	public void run(String strJson){
		jsonObj = buildObj(strJson);
	}

	private JsonArray buildArr(String strArr){
		JsonArray jsonArray = new JsonArray();

		char[] cbrace = new char[]{'[', ']'};
		LinkedList<String> liToken = SchemaBuilder.tokenize(strArr, cbrace); //Tokenize a array closing by a couple of square brackets
		int nliToken = 0;     //nliToken counts the index of current element
		for(String str : liToken){
			stkPath.push(nliToken++);

			str = SchemaBuilder.tailorStr(str);
			String strType = SchemaBuilder.getType(str);
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
	public JsonObject buildObj(String strJson){
		JsonObject jsonObject = new JsonObject();

		char[] cbrace = new char[]{'{', '}'};
		List<String> liToken = SchemaBuilder.tokenize(strJson, cbrace);
		for(String str : liToken){
			List<String> pair = SchemaBuilder.toKeyValPair(str);
			stkPath.push(pair.get(0));

			JsonElement jsonElement;
			String strType = SchemaBuilder.getType(pair.get(1));   //return the type of value
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


	static void initialList(LinkedList<Integer> liBranchLoc, LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt
			, LinkedList<Object> liPath){
		for(int nInd = 0; nInd != liPath.size() - 1; ++nInd){
			if(liPath.get(nInd + 1) instanceof Integer){
				liBranchLoc.push(nInd + 1);
				LinkedList<Object> liPathTmp = new LinkedList<>();
				for(int nInd1 = 0; nInd1 != nInd + 1; ++nInd1){
					liPathTmp.add(liPath.get(nInd1));
				}
				LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
				getJsonElement(liPathTmp, jsonEleContainer); //Because liPath leads to an existing element
													 //all it's precedent elements must be existing
											         //so liPathTmp is leading to an existing jsonEle1
				liBranchSize.push(((JsonArray)jsonEleContainer.getLast().objVal).liMem.size());
				liBranchCnt.push(0);
			}
		}	
		if(!liBranchCnt.isEmpty()){
			liBranchCnt.set(liBranchCnt.size() - 1, -1);
		}
	}

	static boolean incrementBranch(LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt, 
			int nlevel){
		boolean boverflow = true;
		for(int nliInd = 0; nliInd != liBranchCnt.size(); ++nliInd){
			if(liBranchCnt.get(nliInd) < liBranchSize.get(nliInd) - 1){
				boverflow = false;
				break;
			}
		}
		if(boverflow){
			return false;
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
		//repeated contains the semantic of optional
		if(jsonEle.strType.equals("reapeated")){
			return false;
		}

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>();
		
		initialList(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		
		int nBranchNum = liBranchCnt.size();
		if(nBranchNum == 0){
			return false;
		}
		else{
			do{
				if(!incrementBranch(liBranchSize, liBranchCnt, nBranchNum - 1)){
					return false;
				}
				else{
					for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
						liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
					}
					LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
					if(getJsonElement(liPath, jsonEleContainer)){
						if(jsonEleContainer.getLast().objVal == null){
							return true;
						}
					}
				}
			}
			while(true);
		}
		
	}
	static String getJsonValType(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>();
		
		initialList(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		
		int nBranchNum = liBranchCnt.size();
		do{
			if(!incrementBranch(liBranchSize, liBranchCnt, nBranchNum - 1)){
				return "NULL";
			}
			
			for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
				liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
			}
			LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
			if(getJsonElement(liPath, jsonEleContainer)){
				if(jsonEleContainer.getLast().objVal != null){
					return jsonEleContainer.getLast().strType;
				}
			}
		}
		while(true);
	}	
	/*
	 * We assume parameter liPath have to point to a json element(key-value pair), not a naked value
	 */
	static boolean getJsonElement(LinkedList<Object> liPath, LinkedList<JsonElement> jsonEleContainer){
		Object objCur = jsonObj;

		for(Object objLoc : liPath){
			if(objLoc instanceof String){
				if(objCur instanceof JsonObject){
					String strLoc = (String)objLoc;
					JsonObject jsonObj = (JsonObject)objCur;
					if(!jsonObj.getJsonElement(strLoc, jsonEleContainer)){
						return false;
					}
					objCur = jsonEleContainer.getLast().objVal;
				}
				else{
					return false;
				}
			}
			else if(objLoc instanceof Integer){
				if(objCur instanceof JsonArray){
					Integer nLoc = (Integer)objLoc;
					JsonArray jsonArr = (JsonArray)objCur;
					LinkedList<Object> objValContainer = new LinkedList<>();
					if(!jsonArr.getVal(nLoc, objValContainer)){
						return false;
					}
					objCur = objValContainer.getLast();
				}
				else{
					return false;
				}
			}
		}
		
		return true;
	}
	/*
	 * Represent every "key: value" pair in Json object, no matter what level this object obtains
	 * This object could be the top object which is simply the input Json string itself, a "value" object which
	 * is conducted by a key string, a nested-array object which is a element in a array
	 * 
	 * Constructor
	 * @param objVal: objVal is an Object object this JsonElement contains as a value. It could be a JsonArray object,
	 * a JsonObject object, a primitive object
	 * @param strType: strType is the type of the value
	 * @param stk: stk contains the location(also known as "path") of this JsonElement including the key string, and that's 
	 * why JsonElement doesn't contain a member object representing it's key string such as strKey. In the body of 
	 * the constructor, we build the path using stk. 
	 * A path consists of location of all levels starting from top level json object key string to a JsonElement's own
	 * key string, when it comes to a location in an array, an index is used to represent the this location rather than
	 * a key string name, because there is no name string.
	 */
	class JsonElement {
		JsonElement(Object objVal, String strType, Stack<Object> stk){
			this.objVal = objVal;
			this.strType = strType;
			for(Object objPath : stk){
				liPath.add(objPath);
			}
		}

		String strType;
		Object objVal;
		LinkedList<Object> liPath = new LinkedList<>(); //pathList contains the path from top-level object to this element
										//The element in this list could be either Integer representing a
										//index of this in a array, or String representing a name of a key-value
	}
	/*
	 * Almost like JsonArray, the only different point is liMem here cache JsonElement
	 */
	class JsonObject {
		void addPair(JsonElement jsonElement){
			liMem.add(jsonElement);
		}
		
		/*
		 * @param strKey: strKey is the key string of the Json element of this Json object to be retrieved
		 * @param jsonEle: jsonEle is a reference referring to the item retrieved by this function
		 */
		boolean getJsonElement(String strKey, LinkedList<JsonElement> jsonEleContainer){
			jsonEleContainer.clear();
			jsonEleContainer.push(null);
			for(JsonElement jsonEle : liMem){
				if(jsonEle.liPath.getLast().equals(strKey)){
					jsonEleContainer.set(0, jsonEle);
					return true;
				}
			}
			return false;
		}

		LinkedList<JsonElement> liMem = new LinkedList<>();
	}
	/*
	 * JsonArray is a kind of aggregate value type. Since array(repeated) data only appear at the location of
	 * a value position, we consider it as another type of data alongside primitive type(INT32, Float, binary, boolean)
	 * 
	 * @member liMem: liMem is a list caching a series of data of same data type although a slice of them could be
	 * null. The type here could be aggregate or primitive 
	 */
	class JsonArray {
		void addValue(Object obj){
			liMem.add(obj);
		}
		boolean getVal(Integer nInd, LinkedList<Object> objValContainer){
			objValContainer.clear();
			objValContainer.push(null);
			objValContainer.set(0, liMem.get(nInd));
			return objValContainer.getLast() == null ? false : true;
		}
		LinkedList<Object> liMem = new LinkedList<>();
	}
}
