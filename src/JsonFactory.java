import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/*
 *@class JsonFactory JsonFactory�ĳ�Աrun��������һ������json�ַ������ַ�������strJson
 *����buildObj��������JsonFactory���ڲ���JsonObject�����json���󣬲����ظö���
 */
public class JsonFactory {
	static JsonObject jsonObj;       //��ǰ���ڴ���Ķ���Ԫ��
	static JsonObject jsonObjCur;     //Ŀǰ����ȱֵ��û��ĳ��ֵ�ԣ���Ԫ�飬�����null�jsonObjʱ������
	static Stack<Object> stkPath = new Stack<>(); //Static stack tracing global path

	public void run(String strJson){
		jsonObj = buildObj(strJson);
		fillObjWithNull(jsonObj);
	}

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
	 * ���ֵ��jsonEle�ĶԵȼ�ֵ�����nullֵ
	 */
	private void fillPeerWithNull(JsonElement jsonEle){
		LinkedList<Object> liPath = new LinkedList<Object>(jsonEle.liPath);

		LinkedList<Integer> liBranchLoc = new LinkedList<>();
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>(); 
		
		initialList(liBranchLoc, liBranchSize, liBranchCnt, liPath);
		int nBranchNum = liBranchSize.size();
		
		LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();  //û���ϣ�ֻ��Ϊ�˵���getJsonElement����
		while(incrementBranch(liBranchSize, liBranchCnt, nBranchNum - 1)){
			for(int nliInd = 0; nliInd != nBranchNum; ++nliInd){
				liPath.set(liBranchLoc.get(nliInd), liBranchCnt.get(nliInd));
			}
			
			//���û���ҵ������ֵ��
			if(!getJsonElement(liPath, jsonEleContainer)){
				jsonObjCur.addPair(new JsonElement(null, "NULL", stkPath));
			}
		}
	}
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
	 * json��ֵ�Զ�����JsonFactory���ڲ���JsonElement���壬����liPath��Ա�Ǹü�ֵ��������json�����е�·����
	 * ÿһ����ֵ�Զ����·���ɴӶ���JsonObject�����еļ�ֵ��������ļ�ֵ��������ɣ������·������ĳһ���飬��
	 * ���ö����ڸ������е����Ƚ���λ����Ϊ��·��������һ����ʱ��λ�á�
	 * ��·����������ʱ�����ǳ�·���ڴ˴��ֲ棨branch��
	 * 
	 * @func initialList: initialList������liPath���е�branch�㰴�ճ��ֵ��Ⱥ�˳�򣬷ֱ���liBranchLoc
	 * ��liBranchSize��liBranchCnt���������У�liBranchLoc��Ÿ�branch����liPath�е�λ�ã��Ӷ���JsonObject��
	 * ��ֵ���𣩣�liBranchSize��Ÿ�branch�������Ԫ�ظ�����liBranchCnt�ж�Ӧ��λ�ó�ʼ��Ϊ��
	 */
	static void initialList(LinkedList<Integer> liBranchLoc, LinkedList<Integer> liBranchSize, LinkedList<Integer> liBranchCnt
			, LinkedList<Object> liPath){
		for(int nInd = 0; nInd != liPath.size() - 1; ++nInd){
			//���liPath��nInd+1����λ������Ϊ���ͣ���˵���ô���ŵ���branch��
			if(liPath.get(nInd + 1) instanceof Integer){
				liBranchLoc.push(nInd + 1);

				//ȡ������branch���Ӧ��ֵΪ�����json��ֵ�ԣ��õ������С
				LinkedList<Object> liPathTmp = new LinkedList<>();
				for(int nInd1 = 0; nInd1 != nInd + 1; ++nInd1){
					liPathTmp.add(liPath.get(nInd1));
				}
				
				//��ΪJava�������õĲ�������ֵ���ݣ����Խ�������JsonElement�������õ�LinkedList���в�������
				//����������getJsonElement�����������ڴ�ŵ�Ψһһ��JsonElemnt���͵�����ָ����Ǽ�ֵ�Զ���
				LinkedList<JsonElement> jsonEleContainer = new LinkedList<>();
				getJsonElement(liPathTmp, jsonEleContainer); 
				liBranchSize.push(((JsonArray)jsonEleContainer.getLast().objVal).liMem.size());
				liBranchCnt.push(0);
			}
		}	

		//�˴��ں���˵��
		if(!liBranchCnt.isEmpty()){
			liBranchCnt.set(liBranchCnt.size() - 1, -1);
		}
	}

	/*
	 * ��liBranchCnt�ĵ�nlevel-1��Ԫ�ؼ�1�����liBranchCnt.get(nlevel - 1) + 1 == liBranchSize.get(nlevel - 1)
	 * ��liBranchCnt�ĵ�nlevel-1��Ԫ�����㣬��nlevel-2��Ԫ����+1
	 * ������ۼ�֮ǰ����liBranchCnt��λ���Ѵﵽ���ֵ���������������false������ۼӳɹ�����true
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
		LinkedList<Integer> liBranchSize = new LinkedList<>();
		LinkedList<Integer> liBranchCnt = new LinkedList<>(); //liBranchCnt ��ʾ·���ڵ�ǰ�����鴦��λ��ֵ
		//����ÿ�λ�ȡ�Եȼ�ֵ����ʱ��Ҫ�����Ӷ����·��ֵ������ӵ�0��·������do-whileѭ�������0��·���ϵ�
		//ֵ��Զ�޷�ȡ����������initialList������������·���д���branch��㣨liBranchCnt�ĳ��Ȳ�Ϊ0����
		//���Ƚ���ʾ��ǰ����·����liBranchCnt�����һ��Ԫ������Ϊ-1
		
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
						//���liPathָʾ��ֵ������,��˵���ü�ֵ�Զ�����ڱ����ԵĶ���,�ü�ֵ��Ϊoptional
						return true;
					}
				}
			}
			while(true);
		}
		
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

        //�����ֵ��jsonObjһ���жԵȼ�ֵ��,���û�����쳣
		throw new RuntimeException("No such peer json element"); 
	}
	/*
	 * ͨ�������ֵ��·����liPath���ҵ���Ӧ�ļ�ֵ�Զ�������ɹ�����true����ͨ��jsonEleContainer����
	 * ����ָ��ö�������ã����ʧ�ܷ���false�����liPathָ��һ������ֵ�����޷���ü�ֵ���󣬷���false
	 */
	static boolean getJsonElement(LinkedList<Object> liPath, LinkedList<JsonElement> jsonEleContainer){
		stkPath.clear();  //׷�ٵ�ǰ·��

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
	 * JsonElement�ඨ���˼�ֵ����
	 * strTypeΪ�ü�ֵ�Ե�ֵ�����ͣ�group��repeated��INT32��FLOAT��BINARY����BOOLEAN��NULL��
	 * Object���͵�objVal�����ֵ��ʵ�壬��Ϊֵ�����ͣ����������Բ���Object���ʹ洢
	 * liPath�����Ӷ���json����ļ�ֵ���ü�ֵ�Եļ�ֵ��·����������ܹ����ɵ���������ֻ����ΪInteger��String
	 * ���ͣ�Integer���ʹ���·���������е�λ�ã�String���ʹ���·���ھ�����ĳһ��ֵ�Եļ�ֵ
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
