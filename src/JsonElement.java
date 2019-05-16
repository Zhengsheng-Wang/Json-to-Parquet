import java.util.LinkedList;
import java.util.Stack;

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

public class JsonElement {
	public JsonElement(Object objVal, String strType, Stack<Object> stk){
		this.objVal = objVal;
		this.strType = strType;
		liPath = new LinkedList<>();
		for(Object objPath : stk){
			liPath.add(objPath);
		}
	}

	String strType;
	Object objVal;
	LinkedList<Object> liPath; //pathList contains the path from top-level object to this element
									//The element in this list could be either Integer representing a
									//index of this in a array, or String representing a name of a key-value
}