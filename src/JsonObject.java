import java.util.LinkedList;

/*
 * Almost like JsonArray, the only different point is liMem here cache JsonElement
 */
public class JsonObject {
	public void addPair(JsonElement jsonElement){
		liMem.add(jsonElement);
	}
	
	/*
	 * @param strKey: strKey is the key string of the json element of this json object to be retrieved
	 * @param jsonEle: jsonEle is a reference refering to the item retrieved by this function
	 */
	public boolean getJsonElement(String strKey, JsonElement jsonEle){
		for(JsonElement jsonEle1 : liMem){
			if(jsonEle1.liPath.get(0).equals(strKey)){
				jsonEle = jsonEle1;
				return true;
			}
		}
		return false;
	}

	LinkedList<JsonElement> liMem = new LinkedList<>();
}
