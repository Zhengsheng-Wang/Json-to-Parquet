import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Stack;

public class JsonFormater {
	public static List<String> format(String strFile) throws IOException{
		List<String> liStr = new LinkedList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(new File(strFile)));

		/*
		 * 每行数据读入strLine中
		 * strJson用于存储当前解析出的json数据，由于文件strFile中的json数据在文件中所占的位置有以下两种情况：
		 * 1 一个json数据占多行
		 * 2 在文件的一行出项多个json数据
		 * 
		 * 事实上，每个json数据都是由一对{}包起来的，我们用一对{}来隔离出json数据（也有可能是被嵌套的元组）。
		 * 用一个字符栈brk将每次碰到的‘{’入栈，如果碰到了‘}’，则说明找到了与前一个入栈的‘{’相匹配的‘}’，
		 * 即找到了一个元组，那么将最近一次入栈的‘{’弹出。
		 * 每次弹出完，检测brk是否为空。如果不为空，继续读入下一个字符；
		 * 如果为空，则当前被存入strJson的字符可被当成一个json数据，存入
		 * 链表liStr，接着清空strJson，进行下一次解析。
		 * 
		 * 返回链表liStr，其中每一个元素为解析出的json数据
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

		reader.close();
		return liStr;
	}
}
