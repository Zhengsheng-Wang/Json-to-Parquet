import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Stack;

public class JsonFomater {
	public static List<String> format(String strFile) throws IOException{
		List<String> liStr = new LinkedList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(new File(strFile)));

		/*
		 * For now, we merge the json data occupying multilines 
		 * into one line
		 * Assume a line only contains one object of json data.
		 */
		String strLine = "";
		String strJson = "";
		Stack<Character> brk = new Stack<Character>();
		while((strLine = reader.readLine()) != null){
			for(char c : strLine.toCharArray()){
				strJson += c;
				if(c == '{'){
					brk.push('{');
				}
				else if(c == '}'){
					brk.pop();
					if(brk.empty()){
						liStr.add(strJson);
						strJson = "";
					}
				}
			}
		}

		return liStr;
	}
}
