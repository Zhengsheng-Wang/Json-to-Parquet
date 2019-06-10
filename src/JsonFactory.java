import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/*
 *@class JsonFactory 建立run函数接收的json数据strJson的内部表示
 *
 *为了能够支持json数据中的所有大小的数值型数据元素，我们将Json数据中的
 *所有整型和浮点型分别按照为Long型和Double型存储于内部表示中，但在内部表示的JsonElement.strType
 *中还是显示为INT32和FLOAT
 *
 *写入parquet文件时，将两者分别转化为Int型和Float型存入
 */
public class JsonFactory {
	static JsonObject jsonObj;       //当前正在处理的顶层元组
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	/*
	 *@func  run函数接收一个代表json数据的字符串常量strJson
	 *	         调用buildObj函数建立JsonFactory的内部类JsonObject定义的json对象
	 */
	public void run(String strJson){
		stkPath.clear();
		jsonObj = buildObj(strJson);
		fillObjWithNull(jsonObj);    //向jsonObj中的漏值处（本来应该有键值对的地方）添加类型为NULL的键值对
		long2doubleInObject(jsonObj);  //支持整型类型自动提升为Double型
		setNullInJsonObject(jsonObj); 
	}

	///////////////////////////////////////
	/*
	 * 因为无法向hdfs写入空的元组，所以将所有空元组赋值为空
	 */
	private void setNullInJsonObject(JsonObject jsonObj){
		for(int nInd = 0; nInd != jsonObj.liMem.size(); ++nInd){
			JsonElement jsonEle = jsonObj.liMem.get(nInd);
			
			if(jsonEle.strType.equals("group")){
				if(((JsonObject)jsonEle.objVal).liMem.isEmpty()){
					jsonEle.objVal = null;
					jsonEle.strType = "NULL";
				}
				else{
					setNullInJsonObject((JsonObject)jsonEle.objVal);  //如果当前元素也为元组，则递归调用
				}
			}
		}
	}
	//////////////////////////////////////
	/*
	 * 依次检查Long类型键值对jsonEle的对等键值对，如果发现有对等键值对类型为Double，则将当前键值对jsonEle的类型
	 * 提升为Double
	 */
	private void long2double(JsonElement jsonEle){
		if(jsonEle.strType.equals("INT32")){
			LinkedList<Object> liPath = new LinkedList<>(jsonEle.liPath);
			
			LinkedList<Integer> liBranchLoc = new LinkedList<>();
			initialList(liBranchLoc, liPath);
			
			LinkedList<Object> objContainer = new LinkedList<>();
			do{
				if(getObj(liPath, objContainer)){
					if(((JsonElement)objContainer.getLast()).strType.equals("FLOAT")){
						jsonEle.objVal = 
								new Double(((Long)jsonEle.objVal).doubleValue());
						jsonEle.strType = "FLOAT";
						break;
					}
				}
			}while(incrementBranch(liBranchLoc, liPath));
		}
	}
	/*
	 * 依次对数组jsonArr内的元素进行类型检查，有一下三种情况：
	 * 1，元素为数组，则递归调用
	 * 2，元素为元组，则调用long2DoubleInObject
	 * 3，元素为Long值，则：先遍历此元素的所有对等数组元素，如果发现有Double类型的对等数组元素，则将此元素以及与它处于
	 *    同一数组的后面的类型为Long的元素均提升为Double型数据
	 */
	private void long2doubleInArray(JsonArray jsonArr, LinkedList<Object> liPath){
		LinkedList<Object> liPath1 = new LinkedList<>(liPath);

		int nLoc = 0;
		for(Object obj : jsonArr.liMem){
			liPath1.add(nLoc);
			if(obj instanceof JsonArray){
				long2doubleInArray((JsonArray)obj, liPath1);
			}
			else if(obj instanceof JsonObject){
				long2doubleInObject((JsonObject)obj);
			}
			else if(obj instanceof Long){
				LinkedList<Integer> liBranchLoc = new LinkedList<>();
				
				initialList(liBranchLoc, liPath1);
				LinkedList<Object> objContainer = new LinkedList<>();

				//遍历它的同等数组元素
				do{
					if(getObj(liPath1, objContainer)){
						Object obj1 = objContainer.getLast();
						//发现一个对等数组元素类型为Double
						if(obj1 instanceof Double){
							//提升本元素的类型至Double
							jsonArr.liMem.set(nLoc, new Double(((Long)obj).doubleValue()));
							
							//挨个提升与其属于同一数组的后面的元素的类型
							if(jsonArr.liMem.size() > nLoc + 1){
								int nLoc1 = nLoc + 1;
								for(Object obj2 : jsonArr.liMem.subList(nLoc + 1, jsonArr.liMem.size())){
									if(obj2 instanceof Long){
										jsonArr.liMem.set(nLoc1, new Double(((Long)obj2).doubleValue()));
									}
									++nLoc1;
								}
								return;
							}
							else{
								return;
							}
						}
					}
				}while(incrementBranch(liBranchLoc, liPath1));
			}
			
			liPath1.removeLast();
			++nLoc;
		}
	}
	/*
	 * 把元组中含有double类型的对等键值对的long类型的键值对转化为double类型
	 */
	private void long2doubleInObject(JsonObject jsonObj){
		for(JsonElement jsonEle : jsonObj.liMem){
			if(jsonEle.strType.equals("repeated")){
				long2doubleInArray((JsonArray)jsonEle.objVal, jsonEle.liPath);
			}
			else if(jsonEle.strType.equals("group")){
				long2doubleInObject((JsonObject)jsonEle.objVal);
			}
			else if(jsonEle.strType.equals("INT32")){
				long2double(jsonEle);
			}
		}
	}
	///////////////////////////////////
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
			fillPeerWithNull(jsonEle);   //先将当前键值对的对等空键值对赋值为null
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
		
		initialList(liBranchLoc, liPath);
		LinkedList<Object> objContainer = new LinkedList<>();
		
		do{
			if(!getObj(liPath, objContainer)){
				/*
				 * 如果没有取得有效值，由于空数组对取值的影响只会存在于在incrementBranch中查询数组大小的时候，
				 * 现在查找的都是键值对，所以阻碍查找到liPath指示值的情况只可能有以下两种：
				 * 1.没有对等键值对
				 * 2.对等键值对的类型为NULL
				 */
				Object objLastValid = objContainer.getLast();
				if(objLastValid instanceof JsonElement){
					//最后取得的有效值是键值对
					JsonElement jsonEleLastValid = (JsonElement)objLastValid;
					if(jsonEleLastValid.strType.equals("group")){
						//键值对的类型是元组，这对应于情况1，该元组中没有对等键值对，需要添加
						JsonObject jsonObjLastValid = (JsonObject)jsonEleLastValid.objVal;
						jsonObjLastValid.addPair(new JsonElement(null, "NULL", stkPath));
					}
					//其它的情况应该是“键值对的类型是NULL”，这时就不用添加null了
				}
				else{
					//否则，最后取得的有效值是在一个数组中的元组，并且这个元组不包含对等键值对，需要添加
					JsonObject jsonObjLastValid = (JsonObject)objLastValid;
					jsonObjLastValid.addPair(new JsonElement(null, "NULL", stkPath));
				}
			}
		}while(incrementBranch(liBranchLoc, liPath));
	}
	///////////////////////////////////////
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
				jsonArray.addValue(Long.parseLong(str));
				break;
			case "FLOAT":
				jsonArray.addValue(Double.parseDouble(str));
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
				jsonElement = new JsonElement(Long.parseLong(pair.get(1)), strType, stkPath);
				jsonObject.addPair(jsonElement);
				break;
			case "FLOAT":
				jsonElement = new JsonElement(Double.parseDouble(pair.get(1)), strType, stkPath);
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
	void initialList(LinkedList<Integer> liBranchLoc, LinkedList<Object> liPath){
		for(int nInd = 1; nInd != liPath.size(); ++nInd){
			/*
			 *因为json数据的数组不可能直接出现在json数据中，在最上面一定有一个键值，所以nInd从1而不是0开始
			 *如果liPath在nInd处的位置类型为整型，则说明该处存放的是branch点。
			 *在找到branch点的同时，将liPath的branch点赋值为零
			*/
			if(liPath.get(nInd) instanceof Integer){
				liBranchLoc.add(nInd);
				liPath.set(nInd, 0);
			}
		}	
	}

	boolean incrementBranch(LinkedList<Integer> liBranchLoc, LinkedList<Object> liPath){
		//如果没有 经过数组结点直接返回false退出
		if(liBranchLoc.size() == 0){
			return false;
		}

		LinkedList<Object> liPathTmp = new LinkedList<>(); //liPathTmp存储寻找到数组结点的临时路径

		/*
		 * liPath上branch结点的数值表示在路径经过某一数组时选择了其中的第几个元素
		 * 外层for循环在路径liPath的第liBranchLoc.get(nInd)个结点上加1，在每次循环中先获取这个结点上可能的最大值
		 * ，如果当前结点值增加1后大于最大值，则将该结点值置0，进入下一层循环（进位）。并返回true，表示递增成功。
		 * 如果最高位在加1后会溢出，则返回false表示递增失败
		 */
		for(int nInd = liBranchLoc.size() - 1; nInd != -1; --nInd){
			int nLoc = (Integer)liPath.get(liBranchLoc.get(nInd));

			liPathTmp.clear();
			//获得到该数组结点的路径，存入liPathTmp
			for(int nInd1 = 0; nInd1 != liBranchLoc.get(nInd); ++nInd1){
				liPathTmp.add(liPath.get(nInd1));
			}
			
			LinkedList<Object> objContainer = new LinkedList<>();
			//目前看来，getObj一定返回true
			if(getObj(liPathTmp, objContainer)){
				//objPnt是取出的包含当前分支点的对象。可能是一个值为数组的键值对，也可能是一个数组
				//取出当前数组的元素个数，即分支点的最大值
				Object objPnt = objContainer.getLast();
				int nSize = 0;
				if(objPnt instanceof JsonElement){
				//如果父结点是数组键值对
					JsonElement jsonEle = (JsonElement)objPnt;
					if(!jsonEle.strType.equals("NULL")){
						//当这个父键值对的值不为null时
						nSize = ((JsonArray)((JsonElement)objContainer.getLast()).objVal).liMem.size();
					}
					else{
						nSize = 0;   //父键值对的值为null，数组 大小设为0
					}
				}
				//如果父结点是数组值，也就是说当前结点是一个嵌套在数组里的数组
				else{
					nSize = ((JsonArray)objPnt).liMem.size();
				}
				
				if(nLoc + 1 == nSize || nSize == 0){
					/*
					 *如果在当前分支点上增1后超出了当前数组的元素个数(nLoc + 1 == nSize)
					 *或者当前键值对的值为null（nSize == 0），或者当前数组的大小为0(nSize == 0)
					 */
					if(nInd != 0){
						//如果目前不正在第0个分支点上增1，则直接进位
						//（将本分支点置零，进入下一循环——在上一个分支点增1）
						liPath.set(liBranchLoc.get(nInd), 0);
						continue;
					}
					else{
						//如果目前在第零个分支点上增1，则说明再加1就要溢出了，这次递增失败，返回false
						return false;
					}
				}
				else{
					//如果在当前分支点上增1后没有超出当前数组元素个数，则直接增1
					liPath.set(liBranchLoc.get(nInd), nLoc + 1);
					return true;
				}
			}
			else{
				/*
				 * 这里的getObj取出的应该是一个数组
				 * 如果此处的getObj无法取得有效值，只可能是因为这种情况：上一次在高位w上递增后，
				 * liPath在w位之后的所有分支节点上的值全为0，但是w位之后的结点有可能出现：NULL类型键值对、
				 * 没有对等键值对、空数组(用0去索引空数组是无法索引成功的)这几种阻碍取得liPath指示位置值的情况
				 * 如果出现了这种情况，根据最后一次请求成功的值的类型做出不同反应
				 * 
				 * 如果没有找到这个键值对的对等键值对(这个键值对的键在某一待查找的元组中根本不存在),
				 * 则向阻碍这个对等键值对查找路径的元组添加值为null的键值对,键名由stkPath决定。
				 * stkPath存放的是本以为该元组中应该存在的键值.
				 */
				Object objLastValid = objContainer.getLast();   //objLastValid是最后一次取得的有效的元素
				if(objLastValid instanceof JsonElement){
					//如果最后一次取得的有效元素是键值对
					JsonElement jsonEle = (JsonElement)objLastValid;
					if(jsonEle.strType.equals("group")){
						/*
						 * 如果最后一次取得的有效键值对是元组类型，也就是说这个元组下面不包括这个键值对
						 * 那么添加一个类型为NULL的同名键值对
						 */
						JsonObject jsonObj = (JsonObject)jsonEle.objVal;
						jsonObj.addPair(new JsonElement(null, "NULL", stkPath));
					}
					else if(jsonEle.strType.equals("repeated")){
						/*
						 * 如果最后一次取得的有效键值对是数组类型，也就是说这是一个空数组
						 * 不做任何操作
						 */
					}
					//找到那个出现问题的分叉节点的位置，并将其加一后赋给nInd，因为最外层的for循环要减一
					//所以此时的nInd个结点就是这个问题结点
					//在第nInd个结点上尝试增一
					nInd = getLastBranchIndexOfPath(stkPath);
				}
				else if(objLastValid instanceof JsonObject){
					/*
					 * 如果最后一次查找成功的值是存在于某一个数组内，说明
					 * 这个值是一个作为一个数组元素的元组，之所以没有找到值，是因为这个元组没有包含
					 * 这个键值对。
					 * 添加类型为NULL的同名键值对
					 */
					JsonObject jsonObj = (JsonObject)objLastValid;
					jsonObj.addPair(new JsonElement(null, "NULL", stkPath));
					//理由同上
					nInd = getLastBranchIndexOfPath(stkPath);
				}
				else if(objLastValid instanceof JsonArray){
					/*
					 * 如果最后一次查找成功的值就是一个数组，说明，查找没有成功的值
					 * 是一个嵌套在这个数组里的空数组
					 */
					stkPath.pop();
					nInd = getLastBranchIndexOfPath(stkPath);
				}
			}
		}
		
		return false;
	}
	/*
	 * 找到一个路径包含的数组结点索引的个数
	 */
	int getLastBranchIndexOfPath(Stack<Object> liPath){
		int n = 0;
		for(Object obj : liPath){
			if(obj instanceof Integer){
				++n;
			}
		}
		return n - 1;
	}
	

	/*
	 * 查询传入的JsonElement是否为optional，如果这个键值对对象的值类型为数组类型，则返回结果为false
	 * 因为数组类型repeated包含optional的语义
	 * 遍历这个键值对象的在整个json对象中的所有对等键值对象，如果有一个键值对象为null，则说明该键值对象的属性
	 * 为optional
	 */
	boolean queryOptional(JsonElement jsonEle){
		if(jsonEle.strType.equals("reapeated")){
			return true;
		}

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		boolean bNullExisting = false;  //有对等值类型为NULL、或某一对等键值根本不存在的标志
		boolean bValidExisting = false;  //有对等值是有效类型的标志，为了排除所有对等值和这个键值对自己都是null的情况 
		LinkedList<Object> objContainer = new LinkedList<>();
		do{
			if(getObj(liPath, objContainer)){
				//只有当能够通过这一次的liPath取出对等有效键值对时，才可能判断出
				//键值对是否会因为这个有效键值对而变得optinal
				//如果键值对jsonEle有属性为repeated的对等键值对，返回false
				if(((JsonElement)objContainer.getLast()).strType.equals("repeated")){
					return true;
				}
				else if(((JsonElement)objContainer.getLast()).strType.equals("NULL")){
					bNullExisting = true;
				}
				else{
					//如果取得的值既不是数组类型，也不是NULL类型，那证明存在有效值，这个键值对可以是optional的
					bValidExisting = true;
				}
			}
		}while(incrementBranch(liBranchLoc, liPath));
		
		return bNullExisting && bValidExisting;
	}
	/////////////////
	/*
	 * 得到键值对的值的类型，如果传入的jsonEle的值为null，则遍历它的对等键值对，判断它该有的
	 * 类型
	 */
	String getJsonValType(JsonElement jsonEle){
		//如果jsonEle的类型不为NULL，直接返回其类型即可
		if(!jsonEle.strType.equals("NULL")){
			return jsonEle.strType;
		}
		
		//如果jsonEle类型为NULL，则尝试从它的对等键值对推测出它的类型

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		LinkedList<Object> objContainer = new LinkedList<>();
		do{
			if(getObj(liPath, objContainer)){
				if(((JsonElement)objContainer.getLast()).objVal != null){
					return ((JsonElement)objContainer.getLast()).strType;
				}
			}
		}
		while(incrementBranch(liBranchLoc, liPath));
		return "NULL";  //如果没有通过jsonEle的对等键值对找到它的类型，则默认它的类型是NULL
	}	

	/*
	 * 因为getFirstPeerJsonElement函数只在已经确定jsonEle为optinal的时候调用，所以一定能取得有效值
	 */
	JsonElement getFirstPeerJsonElement(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		LinkedList<Object> objContainer = new LinkedList<>();
		do{
			//如果取得键值对成功，并且键值对类型不是NULL
			if(getObj(liPath, objContainer) && !((JsonElement)objContainer.getLast()).strType.equals("NULL")){
				return (JsonElement)objContainer.getLast();
			}
		}while(incrementBranch(liBranchLoc, liPath));

        //假设键值对jsonObj一定有对等键值对,如果没有抛异常
		throw new RuntimeException("No such peer json element"); 
	}
	/*
	 * 通过代表键值对或数组元素路径的liPath，找到对应的键值对或元素，如果成功返回true，并通过objContainer链表
	 * 返回指向该对象的引用，如果失败返回false。
	 */
	static boolean getObj(LinkedList<Object> liPath, LinkedList<Object> objContainer){
		objContainer.clear();
		objContainer.add(null);
		stkPath.clear();  //追踪当前路径

		Object objCur = jsonObj;

		/*
		 * objCur是当前所处的对象，objLoc是在当前所处对象objCur下的路径。
		 * 如果objCur指向元组，则objLoc应该是String类型，表示这个元组下的键值为objLoc的元素
		 * 如果objCur指向数组，则objLoc应该是Integer类型，表示这个数组下的第objLoc个元素
		 */
		for(Object objLoc : liPath){
			stkPath.push(objLoc); //当该次取值失败时，stkPath存放直到这次失败位置的全部路径

			if(objLoc instanceof String){
				if(objCur instanceof JsonObject){
					String strLoc = (String)objLoc;

					LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
					//如果strLoc指示的键值不存在于元组objCur，查找失败
					if(!((JsonObject)objCur).getJsonElement(strLoc, jsonEleContainer)){
						return false;
					}
					objCur = jsonEleContainer.getLast().objVal;
					objContainer.set(0, jsonEleContainer.getLast());  //objContainer存放最近一次的有效键值对
				}
				else{
					return false;
				}
			}
			else if(objLoc instanceof Integer){
				if(objCur instanceof JsonArray){
					Integer nLoc = (Integer)objLoc;
					JsonArray jsonArr = (JsonArray)objCur;
					if(!jsonArr.getVal(nLoc, objContainer)){  //objContianer存放最近一次有效的数组元素
						return false;
					}
					objCur = objContainer.getLast();
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
	 * strType为该键值对的值的类型（group，repeated，INT32，FLOAT，BINARY，BOOLEAN，NULL）
	 * Object类型的objVal，存放值的实体，因为值的类型不定，所以采用Object类型存储
	 * liPath包含从顶层json对象的键值到该键值对的键值的路径，链表的能够容纳的引用类型只可能为Integer和String,
	 * Integer类型代表路径在数组中的位置，String类型代表路径在经过的某一键值对的键值
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
		LinkedList<Object> liPath = new LinkedList<>(); 
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
			if(nInd >= liMem.size()){
				//如果请求的数组元素的坐标超出了数组的元素个数，返回false，并且不改变objValContainer
				return false;
			}
			objValContainer.clear();
			objValContainer.push(null);
			objValContainer.set(0, liMem.get(nInd));
			return objValContainer.getLast() == null ? false : true;
		}
		LinkedList<Object> liMem = new LinkedList<>();
	}
}
