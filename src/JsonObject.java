import java.util.LinkedList;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.spark.sql.execution.ObjectConsumerExec;

import com.fasterxml.jackson.core.Versioned;

/*
 * Almost like JsonArray, the only different point is liMem here cache JsonElement
 */
public class JsonObject {
	public void addPair(JsonElement jsonElement){
		liMem.add(jsonElement);
	}
	public boolean getVal(String strKey, Object objVal){
		for(JsonElement jsonEle : liMem){
			if(jsonEle.liPath.get(0).equals(strKey)){
				objVal = jsonEle.objVal;
				return true;
			}
		}
		return false;
	}

	LinkedList<JsonElement> liMem = new LinkedList<>();
}
