import java.util.LinkedList;

/*
 * JsonArray is a kind of aggregate value type. Since array(repeated) data only appear at the location of
 * a value position, we consider it as another type of data alongside primitive type(INT32, Float, binary, boolean)
 * 
 * @member liMem: liMem is a list caching a series of data of same data type although a slice of them could be
 * null. The type here could be aggregate or primitive 
 */
public class JsonArray {
	public void addValue(Object obj){
		liMem.add(obj);
	}
	public boolean getVal(Integer nInd, Object objVal){
		objVal = liMem.get(nInd);
		return objVal == null ? false : true;
	}
	LinkedList<Object> liMem = new LinkedList<>();
}
