import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/*
 *@class JsonFactory ����run�������յ�json����strJson���ڲ���ʾ
 *
 *Ϊ���ܹ�֧��json�����е����д�С����ֵ������Ԫ�أ����ǽ�Json�����е�
 *�������ͺ͸����ͷֱ���ΪLong�ͺ�Double�ʹ洢���ڲ���ʾ�У������ڲ���ʾ��JsonElement.strType
 *�л�����ʾΪINT32��FLOAT
 *
 *д��parquet�ļ�ʱ�������߷ֱ�ת��ΪInt�ͺ�Float�ʹ���
 */
public class JsonFactory {
	static JsonObject jsonObj;       //��ǰ���ڴ���Ķ���Ԫ��
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	/*
	 *@func  run��������һ������json���ݵ��ַ�������strJson
	 *	         ����buildObj��������JsonFactory���ڲ���JsonObject�����json����
	 */
	public void run(String strJson){
		jsonObj = buildObj(strJson);
		fillObjWithNull(jsonObj);    //
		long2doubleInObject(jsonObj);  //֧�����������Զ�����ΪDouble��
		setNullInJsonObject(jsonObj); 
	}

	/*
	 * ��Ϊ�޷���hdfsд��յ�Ԫ�飬���Խ����п�Ԫ�鸳ֵΪ��
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
					setNullInJsonObject((JsonObject)jsonEle.objVal);  //�����ǰԪ��ҲΪԪ�飬��ݹ����
				}
			}
		}
	}
	//////////////////////////////////////
	/*
	 * ���μ��Long���ͼ�ֵ��jsonEle�ĶԵȼ�ֵ�ԣ���������жԵȼ�ֵ������ΪDouble���򽫵�ǰ��ֵ��jsonEle������
	 * ����ΪDouble
	 */
	private void long2double(JsonElement jsonEle){
		if(jsonEle.strType.equals("INT32")){
			LinkedList<Object> liPath = new LinkedList<>(jsonEle.liPath);
			
			LinkedList<Integer> liBranchLoc = new LinkedList<>();
			initialList(liBranchLoc, liPath);
			
			LinkedList<Object> objContainer = new LinkedList<>();
			while(incrementBranch(liBranchLoc, liPath)){
				if(getObj(liPath, objContainer)){
					if(((JsonElement)objContainer.getLast()).strType.equals("FLOAT")){
						jsonEle.objVal = 
								new Double(((Long)jsonEle.objVal).doubleValue());
						jsonEle.strType = "FLOAT";
						break;
					}
				}
			}
		}
	}
	/*
	 * ���ζ�����jsonArr�ڵ�Ԫ�ؽ������ͼ�飬��һ�����������
	 * 1��Ԫ��Ϊ���飬��ݹ����
	 * 2��Ԫ��ΪԪ�飬�����long2DoubleInObject
	 * 3��Ԫ��ΪLongֵ�����ȱ�����Ԫ�ص����жԵ�����Ԫ�أ����������Double���͵ĶԵ�����Ԫ�أ��򽫴�Ԫ���Լ���������
	 *    ͬһ����ĺ��������ΪLong��Ԫ�ؾ�����ΪDouble������
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

				//��������ͬ������Ԫ��
				while(incrementBranch(liBranchLoc, liPath1)){
					if(getObj(liPath1, objContainer)){
						Object obj1 = objContainer.getLast();
						//����һ���Ե�����Ԫ������ΪDouble
						if(obj1 instanceof Double){
							//������Ԫ�ص�������Double
							jsonArr.liMem.set(nLoc, new Double(((Long)obj).doubleValue()));
							
							//����������������ͬһ����ĺ����Ԫ�ص�����
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
				}
			}
			
			liPath1.removeLast();
			++nLoc;
		}
	}
	/*
	 * ��Ԫ���к���double���͵ĶԵȼ�ֵ�Ե�long���͵ļ�ֵ��ת��Ϊdouble����
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
	////////////////////////////////////
	/*
	 * ������jsonArr�е�Ԫ���null
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
	 * ��Ԫ��jsonObj���ȱֵ���nullֵ
	 */
	private void fillObjWithNull(JsonObject jsonObj){
		for(JsonElement jsonEle : jsonObj.liMem){
			fillPeerWithNull(jsonEle);   //�Ƚ���ǰ��ֵ�ԵĶԵȿռ�ֵ�Ը�ֵΪnull
			if(jsonEle.strType.equals("group")){
				fillObjWithNull((JsonObject)jsonEle.objVal);
			}
			else if(jsonEle.strType.equals("repeated")){
				fillElementInArrayWithNull((JsonArray)jsonEle.objVal);
			}
		}
	}
	/*
	 * ���ֵ��jsonEle�ĶԵȼ�ֵ�����nullֵ
	 */
	private void fillPeerWithNull(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		LinkedList<Object> objContainer = new LinkedList<>();  //û���ϣ�ֻ��Ϊ�˵���getObj����
		while(incrementBranch(liBranchLoc, liPath)){
			//���û���ҵ������ֵ��,���谭����Եȼ�ֵ�Բ���·���ļ�ֵ�Եļ�ֵ��ֵΪnull
			//�谭����·���Ľ��ֻ���Ǽ�ֵ�Խ�㣬��Ϊÿ�ε���·����ʱ���ǰ��� ��ǰ·�����ù������С��
			//������������Խ��
			if(!getObj(liPath, objContainer)){
				Object objPnt = objContainer.getLast();  //�谭��ֵ�Ա��ҵ����ϲ�����Ԫ��
				((JsonObject)objPnt).addPair(new JsonElement(null, "NULL", stkPath));
			}
		}
	}
	///////////////////////////////////////
	/*
	 * @func buildArr buildArr�������մ�������ֵ��'['��']'��־��λ��json�ַ���ð�ź��ֵ�����ַ���strArr��
	 * ����JsonFactory�ڲ���JsonArray�����������󣬲����ظö���
	 */
	private JsonArray buildArr(String strArr){
		JsonArray jsonArray = new JsonArray();

		char[] cbrace = new char[]{'[', ']'};
		LinkedList<String> liToken = SchemaBuilder.tokenize(strArr, cbrace); //�������ַ���strArr���ա���������Ϊ������ֵ

		int nliToken = 0;   //nliToken�����ڴ洢��ǰ������Ǹ������ڵĵ�nliToken��Ԫ��
		for(String str : liToken){
			stkPath.push(nliToken++); //����ǰ��Ԫ��λ��nliToken��ջ, ������ֵ�Ե�·�����ڸ����鴦��λ��ֵ

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
			
			stkPath.pop();  //������ǰλ�ã�׼����ջ��һλ��
		}
		
		return jsonArray;
	}
	public JsonObject buildObj(String strJson){;
		JsonObject jsonObject = new JsonObject();

		char[] cbrace = new char[]{'{', '}'};
		List<String> liToken = SchemaBuilder.tokenize(strJson, cbrace);

		for(String str : liToken){

			List<String> pair = SchemaBuilder.toKeyValPair(str); //����ֵ���ַ���������ð�ŷָ�Ϊ��ֵ��
			stkPath.push(pair.get(0));  //����ǰ��ֵ��Ϊ��ֵ�Զ����·���ڴ˴���ֵ�Ե�λ��ֵ

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
	 * json��ֵ�Զ�����JsonFactory���ڲ���JsonElement���壬����liPath��Ա�Ǹü�ֵ��������json�����е�·����
	 * ÿһ����ֵ�Զ����·���ɴӶ���JsonObject�����еļ�ֵ��������ļ�ֵ��������ɣ������·������ĳһ���飬��
	 * ���ö����ڸ������е����Ƚ���λ����Ϊ��·��������һ����ʱ��λ�á�
	 * ��·����������ʱ�����ǳ�·���ڴ˴��ֲ棨branch��
	 * 
	 * @func initialList: initialList������liPath���е�branch�㰴�ճ��ֵ��Ⱥ�˳�򣬷ֱ���liBranchLoc
	 * ��liBranchSize��liBranchCnt���������У�liBranchLoc��Ÿ�branch����liPath�е�λ�ã��Ӷ���JsonObject��
	 * ��ֵ���𣩣�liBranchSize��Ÿ�branch�������Ԫ�ظ�����liBranchCnt�ж�Ӧ��λ�ó�ʼ��Ϊ��
	 */
	static void initialList(LinkedList<Integer> liBranchLoc, LinkedList<Object> liPath){
		for(int nInd = 1; nInd != liPath.size(); ++nInd){
			/*
			 *��Ϊjson���ݵ����鲻����ֱ�ӳ�����json�����У���������һ����һ����ֵ������nInd��1������0��ʼ
			 *���liPath��nInd����λ������Ϊ���ͣ���˵���ô���ŵ���branch�㡣
			 *���ҵ�branch���ͬʱ����liPath��branch�㸳ֵΪ��
			*/
			if(liPath.get(nInd) instanceof Integer){
				liBranchLoc.add(nInd);
				liPath.set(nInd, 0);
			}
		}	
		
		/*
		 * ��liPath�����һ��·������ΪInteger��·����Ϊ-1
		 */
		for(int nInd = liPath.size() - 1; nInd != -1; --nInd){
			if(liPath.get(nInd) instanceof Integer){
				liPath.set(nInd, -1);
				return;
			}
		}
	}

	static boolean incrementBranch(LinkedList<Integer> liBranchLoc, LinkedList<Object> liPath){
		//���û�� ����������ֱ�ӷ���false�˳�
		if(liBranchLoc.size() == 0){
			return false;
		}

		LinkedList<Object> liPathTmp = new LinkedList<>(); //�洢Ѱ�ҵ����������ʱ·��

		/*
		 * liPath��branch������ֵ��ʾ��·������ĳһ����ʱѡ�������еĵڼ���Ԫ��
		 * ���forѭ����·��liPath�ĵ�liBranchLoc.get(nInd)������ϼ�1����ÿ��ѭ�����Ȼ�ȡ�������ϵĵ�ǰ���ֵ
		 * �������ǰ���ֵ����1��������ֵ���򽫸ý��ֵ��0��������һ��ѭ������λ����������true����ʾ�����ɹ���
		 * ������λ�ڼ�1���������򷵻�false��ʾ���
		 */
		for(int nInd = liBranchLoc.size() - 1; nInd != -1; --nInd){
			int nLoc = (Integer)liPath.get(liBranchLoc.get(nInd));

			liPathTmp.clear();
			//��õ����������·��������liPathTmp
			for(int nInd1 = 0; nInd1 != liBranchLoc.get(nInd); ++nInd1){
				liPathTmp.add(liPath.get(nInd1));
			}
			
			LinkedList<Object> objContainer = new LinkedList<>();
			getObj(liPathTmp, objContainer);   //һ����ȡ����Чֵ

			//ȡ����ǰ�����Ԫ�ظ��������������ֵ
			//objPnt��ȡ���ĸ����
			Object objPnt = objContainer.getLast();
			int nSize = 0;
			//���������������ֵ��
			if(objPnt instanceof JsonElement){
				nSize = ((JsonArray)((JsonElement)objContainer.getLast()).objVal).liMem.size();
			}
			//��������������ֵ��Ҳ����˵��ǰ�����һ��Ƕ���������������
			else{
				nSize = ((JsonArray)objPnt).liMem.size();
			}
			
			if(nLoc + 1 == nSize){
				if(nInd != 0){
					liPath.set(liBranchLoc.get(nInd), 0);
					continue;
				}
				else{
					return false;
				}
			}
			else{
				liPath.set(liBranchLoc.get(nInd), nLoc + 1);
				return true;
			}
		}
		
		return false;
	}
	

	/*
	 * ��ѯ�����JsonElement�Ƿ�Ϊoptional����������ֵ�Զ����ֵ����Ϊ�������ͣ��򷵻ؽ��Ϊoptional
	 * ��Ϊ��������repeated����optional������
	 * ���������ֵ�����������json�����е����жԵȼ�ֵ���������һ����ֵ����Ϊnull����˵���ü�ֵ���������
	 * Ϊoptional
	 */
	static boolean queryOptional(JsonElement jsonEle){
		if(jsonEle.strType.equals("reapeated")){
			return false;
		}

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		boolean bNullExisting = false;  //�жԵ�ֵ����ΪNULL����ĳһ�Եȼ�ֵ���������ڵı�־
		do{
			if(!incrementBranch(liBranchLoc, liPath)){
				break;
			}
			else{
				LinkedList<Object> objContainer = new LinkedList<>();
				if(getObj(liPath, objContainer)){
					//�����ֵ��jsonEle������Ϊrepeated�ĶԵȼ�ֵ�ԣ�����false
					if(((JsonElement)objContainer.getLast()).strType.equals("repeated")){
						return false;
					}
					if(((JsonElement)objContainer.getLast()).strType.equals("NULL")){
						bNullExisting = true;
					}
				}
				else{
					//���liPathָʾ��ֵ������,��˵���ü�ֵ�Զ�����ڱ����ԵĶ���,�ü�ֵ����Ϊoptional
					bNullExisting = true;
				}
			}
		}
		while(true);
		
		return bNullExisting;
	}
	/*
	 * �õ���ֵ�Ե�ֵ�����ͣ���������jsonEle��ֵΪnull����������ĶԵȼ�ֵ�ԣ��ж������е�
	 * ����
	 */
	static String getJsonValType(JsonElement jsonEle){
		if(!jsonEle.strType.equals("NULL")){
			return jsonEle.strType;
		}

		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		do{
			if(!incrementBranch(liBranchLoc, liPath)){
				return "NULL";
			}

			LinkedList<Object> objContainer = new LinkedList<>();
			if(getObj(liPath, objContainer)){
				if(((JsonElement)objContainer.getLast()).objVal != null){
					return ((JsonElement)objContainer.getLast()).strType;
				}
			}
		}
		while(true);
	}	

	static JsonElement getFirstPeerJsonElement(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		
		initialList(liBranchLoc, liPath);
		
		LinkedList<Object> objContainer = new LinkedList<>();
		while(incrementBranch(liBranchLoc, liPath)){
			//���ȡ�ü�ֵ�Գɹ������Ҽ�ֵ�����Ͳ���NULL
			if(getObj(liPath, objContainer) && !((JsonElement)objContainer.getLast()).strType.equals("NULL")){
				return (JsonElement)objContainer.getLast();
			}
		}

        //�����ֵ��jsonObjһ���жԵȼ�ֵ��,���û�����쳣
		throw new RuntimeException("No such peer json element"); 
	}
	/*
	 * ͨ�������ֵ�Ի�����Ԫ��·����liPath���ҵ���Ӧ�ļ�ֵ�Ի�Ԫ�أ�����ɹ�����true����ͨ��objContainer����
	 * ����ָ��ö�������ã����ʧ�ܷ���false��
	 */
	static boolean getObj(LinkedList<Object> liPath, LinkedList<Object> objContainer){
		objContainer.clear();
		objContainer.add(null);
		stkPath.clear();  //׷�ٵ�ǰ·��

		Object objCur = jsonObj;

		/*
		 * objCur�ǵ�ǰ�����Ķ���objLoc���ڵ�ǰ��������objCur�µ�·����
		 * ���objCurָ��Ԫ�飬��objLocӦ����String���ͣ���ʾ���Ԫ���µļ�ֵΪobjLoc��Ԫ��
		 * ���objCurָ�����飬��objLocӦ����Integer���ͣ���ʾ��������µĵ�objLoc��Ԫ��
		 */
		for(Object objLoc : liPath){
			stkPath.push(objLoc);

			if(objLoc instanceof String){
				if(objCur instanceof JsonObject){
					String strLoc = (String)objLoc;

					LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
					if(!((JsonObject)objCur).getJsonElement(strLoc, jsonEleContainer)){
						return false;
					}
					objCur = jsonEleContainer.getLast().objVal;
					objContainer.set(0, jsonEleContainer.getLast());  //objContainer������һ�ε���Ч��ֵ��
				}
				else{
					return false;
				}
			}
			else if(objLoc instanceof Integer){
				if(objCur instanceof JsonArray){
					Integer nLoc = (Integer)objLoc;
					JsonArray jsonArr = (JsonArray)objCur;
					if(!jsonArr.getVal(nLoc, objContainer)){  //objContianer������һ����Ч������Ԫ��
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
	 * JsonElement�ඨ���˼�ֵ����
	 * strTypeΪ�ü�ֵ�Ե�ֵ�����ͣ�group��repeated��INT32��FLOAT��BINARY��BOOLEAN��NULL��
	 * Object���͵�objVal�����ֵ��ʵ�壬��Ϊֵ�����Ͳ��������Բ���Object���ʹ洢
	 * liPath�����Ӷ���json����ļ�ֵ���ü�ֵ�Եļ�ֵ��·����������ܹ����ɵ���������ֻ����ΪInteger��String,
	 * Integer���ʹ���·���������е�λ�ã�String���ʹ���·���ھ�����ĳһ��ֵ�Եļ�ֵ
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
	 * JsonObject�ඨ����json����
	 * addPair���������ļ�ֵ�����������JsonElement��ֵ�Գ�Ա��
	 * getJsonElement�������ݼ�ֵstrKey����ȡ��json�����е���Ӧ��ֵ�Ե�ֵ
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
	 * JsonArray�ඨ����json��ֵ���м�ֵΪ����ļ�ֵ�Ե�ֵ
	 * addValue����������������ֵ��
	 * getVal����ͨ�����������nIndȡ����Ӧ��ֵ��
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
