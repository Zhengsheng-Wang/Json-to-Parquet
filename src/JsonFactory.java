import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/*
 *@class JsonFactory JsonFactory的成员run函数接收一个代表json字符串的字符串常量strJson
 *调用buildObj函数建立JsonFactory的内部类JsonObject定义的json对象，并返回该对象
 */
public class JsonFactory {
	static JsonObject jsonObj;       //当前正在处理的顶层元组
	static JsonObject jsonObjCur;     //目前存在缺值（没有某键值对）的元组，针对用null填补jsonObj时的情形
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	public void run(String strJson){
		jsonObj = buildObj(strJson);
		fillObjWithNull(jsonObj);
	}

	/*
	 * 向数组jsonArr中的元素填补null
	 */
	private void fillElementInArrayWithNull(JsonArray jsonArr){
		for(Object obj : jsonArr.liMem){
			if(obj instanceof JsonArray){
				fillElementInArrayWithNull((JsonArray)obj);
			}
			else if(obj instanceof JsonObject){
				fillObjWithNull((JsonObject)obj);
			}
			else{
				return;
			}
		}
	}
	/*
	 * 向元组jsonObj里的缺值填充null值
	 */
	private void fillObjWithNull(JsonObject jsonObj){
		for(JsonElement jsonEle : jsonObj.liMem){
			fillPeerWithNull(jsonEle);
			if(jsonEle.strType.equals("group")){
				fillObjWithNull((JsonObject)jsonEle.objVal);
			}
			else if(jsonEle.strType.equals("repeated")){
				fillElementInArrayWithNull((JsonArray)jsonEle.objVal);
			}
		}
	}
	/*
	 * 向键值对jsonEle的对等键值对填充null值
	 */
	private void fillPeerWithNull(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>(); 
		
		initialList(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		int nBranchNum = liBranchSize.size();
		
		LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();  //没用上，只是为了调用getJsonElement函数
		while(incrementBranch(liBranchSize, liBranchCnt, nBranchNum - 1)){
			for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
				liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
			}
			
			//如果没有找到这个键值对
			if(!getJsonElement(liPath, jsonEleContainer)){
				jsonObjCur.addPair(new JsonElement(null, "NULL", stkPath));
			}
		}
	}
	/*
	 * @func buildArr buildArr函数接收代表数组值（'['与']'标志的位于json字符串冒号后的值）的字符串strArr，
	 * 产生JsonFactory内部类JsonArray定义的数组对象，并返回该对象
	 */
	private JsonArray buildArr(String strArr){
		JsonArray jsonArray = new JsonArray();

		char[] cbrace = new char[]{'[', ']'};
		LinkedList<String> liToken = SchemaBuilder.tokenize(strArr, cbrace); //将数组字符串strArr按照“，”划分为独立的值

		int nliToken = 0;   //nliToken：用于存储当前处理的是该数组内的第nliToken个元素
		for(String str : liToken){
			stkPath.push(nliToken++); //将当前的元素位置nliToken入栈, 当作键值对的路径中在该数组处的位置值

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
				jsonArray.addValue(str);
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
			
			stkPath.pop();  //弹出当前位置，准备入栈下一位置
		}
		return jsonArray;
	}
	public JsonObject buildObj(String strJson){;
		JsonObject jsonObject = new JsonObject();

		char[] cbrace = new char[]{'{', '}'};
		List<String> liToken = SchemaBuilder.tokenize(strJson, cbrace);

		for(String str : liToken){

			List<String> pair = SchemaBuilder.toKeyValPair(str); //将键值对字符串，按照冒号分割为键值对
			stkPath.push(pair.get(0));  //将当前键值作为键值对对象的路径在此处键值对的位置值

			JsonElement jsonElement;  
			String strType = SchemaBuilder.getType(pair.get(1));   
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
	 * json键值对对象，由JsonFactory的内部类JsonElement定义，它的liPath成员是该键值对象所在json数据中的路径。
	 * 每一个键值对对象的路径由从顶层JsonObject对象中的键值到它本身的键值的链表组成，如果该路径经过某一数组，则
	 * 将该对象在该数组中的祖先结点的位置作为该路径经过这一数组时的位置。
	 * 当路径经过数组时，我们称路径在此处分叉（branch）
	 * 
	 * @func initialList: initialList函数将liPath，中的branch点按照出现的先后顺序，分别导入liBranchLoc
	 * ，liBranchSize，liBranchCnt三个链表中，liBranchLoc存放该branch点在liPath中的位置（从顶层JsonObject的
	 * 键值算起），liBranchSize存放该branch点的数组元素个数，liBranchCnt中对应的位置初始化为零
	 */
	static void initialList(LinkedList<Integer> liBranchLoc, LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt
			, LinkedList<Object> liPath){
		for(int nInd = 0; nInd != liPath.size() - 1; ++nInd){
			//如果liPath在nInd+1处的位置类型为整型，则说明该处存放的是branch点
			if(liPath.get(nInd + 1) instanceof Integer){
				liBranchLoc.push(nInd + 1);

				//取出到该branch点对应的值为数组的json键值对，得到数组大小
				LinkedList<Object> liPathTmp = new LinkedList<>();
				for(int nInd1 = 0; nInd1 != nInd + 1; ++nInd1){
					liPathTmp.add(liPath.get(nInd1));
				}
				
				//因为Java对于引用的参数采用值传递，所以建立容纳JsonElement类型引用的LinkedList进行参数传递
				//这样，经过getJsonElement函数后，链表内存放的唯一一个JsonElemnt类型的引用指向的是键值对对象
				LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
				getJsonElement(liPathTmp, jsonEleContainer); 
				liBranchSize.push(((JsonArray)jsonEleContainer.getLast().objVal).liMem.size());
				liBranchCnt.push(0);
			}
		}	

		//此处在后面说明
		if(!liBranchCnt.isEmpty()){
			liBranchCnt.set(liBranchCnt.size() - 1, -1);
		}
	}

	/*
	 * 将liBranchCnt的第nlevel-1个元素加1，如果liBranchCnt.get(nlevel - 1) + 1 == liBranchSize.get(nlevel - 1)
	 * 则将liBranchCnt的第nlevel-1个元素清零，在nlevel-2个元素上+1
	 * 如果在累加之前发现liBranchCnt各位都已达到最大值，则已溢出，返回false，如果累加成功返回true
	 */
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

	/*
	 * 查询传入的JsonElement是否为optional，如果这个键值对对象的值类型为数组类型，则返回结果为optional
	 * 因为数组类型repeated包含optional的语义
	 * 遍历这个键值对象的在整个json对象中的所有对等键值对象，如果有一个键值对象为null，则说明该键值对象的属性
	 * 为optional
	 */
	static boolean queryOptional(JsonElement jsonEle){
		if(jsonEle.strType.equals("reapeated")){
			return false;
		}

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>(); //liBranchCnt 表示路径在当前此数组处的位置值
		//由于每次获取对等键值对象时都要先增加对象的路径值，如果从第0条路径进入do-while循环，则第0条路径上的
		//值永远无法取出，所以在initialList函数的最后，如果路径中存在branch结点（liBranchCnt的长度不为0），
		//则先将表示当前迭代路径的liBranchCnt的最后一个元素设置为-1
		
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
					else{
						//如果liPath指示的值不存在,则说明该键值对对象存在被忽略的对象,该键值对为optional
						return true;
					}
				}
			}
			while(true);
		}
		
	}
	/*
	 * 得到键值对的值的类型，如果传入的jsonEle的值为null，则遍历它的对等键值对，判断它该有的
	 * 类型
	 */
	static String getJsonValType(JsonElement jsonEle){
		if(!jsonEle.strType.equals("NULL")){
			return jsonEle.strType;
		}

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

	static JsonElement getFirstPeerJsonElement(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>(); 
		
		initialList(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		int nBranchNum = liBranchSize.size();
		
		LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
		while(incrementBranch(liBranchSize, liBranchCnt, nBranchNum - 1)){
			for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
				liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
			}
			
			if(getJsonElement(liPath, jsonEleContainer) && !jsonEleContainer.getLast().strType.equals("NULL")){
				return jsonEleContainer.getLast();
			}
		}

        //假设键值对jsonObj一定有对等键值对,如果没有抛异常
		throw new RuntimeException("No such peer json element"); 
	}
	/*
	 * 通过代表键值对路径的liPath，找到对应的键值对对象，如果成功返回true，并通过jsonEleContainer链表
	 * 返回指向该对象的引用，如果失败返回false。如果liPath指向一个数组值，则无法获得键值对象，返回false
	 */
	static boolean getJsonElement(LinkedList<Object> liPath, LinkedList<JsonElement> jsonEleContainer){
		stkPath.clear();  //追踪当前路径

		Object objCur = jsonObj;

		for(Object objLoc : liPath){
			stkPath.push(objLoc);

			if(objLoc instanceof String){
				if(objCur instanceof JsonObject){
					String strLoc = (String)objLoc;
					JsonObject jsonObj = (JsonObject)objCur;
					jsonObjCur = jsonObj;
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
	 * JsonElement类定义了键值对类
	 * strType为该键值对的值的类型（group，repeated，INT32，FLOAT，BINARY，，BOOLEAN，NULL）
	 * Object类型的objVal，存放值的实体，因为值的类型，不定，所以采用Object类型存储
	 * liPath包含从顶层json对象的键值到该键值对的键值的路径，链表的能够容纳的引用类型只可能为Integer和String
	 * 类型，Integer类型代表路径在数组中的位置，String类型代表路径在经过的某一键值对的键值
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
	 * JsonObject类定义了json对象
	 * addPair方法向对象的键值对链表里添加JsonElement键值对成员。
	 * getJsonElement方法根据键值strKey，获取该json对象中的相应键值对的值
	 */
	class JsonObject {
		void addPair(JsonElement jsonElement){
			liMem.add(jsonElement);
		}
		
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
	 * JsonArray类定义了json键值对中键值为数组的键值对的值
	 * addValue方法向该数组中添加值。
	 * getVal方法通过数组的索引nInd取出对应的值。
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
