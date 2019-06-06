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
		 * ÿ�����ݶ���strLine��
		 * strJson���ڴ洢��ǰ��������json���ݣ������ļ�strFile�е�json�������ļ�����ռ��λ�����������������
		 * 1 һ��json����ռ����
		 * 2 ���ļ���һ�г�����json����
		 * 
		 * ��ʵ�ϣ�ÿ��json���ݶ�����һ��{}�������ģ�������һ��{}�������json���ݣ�Ҳ�п����Ǳ�Ƕ�׵�Ԫ�飩��
		 * ��һ���ַ�ջbrk��ÿ�������ġ�{����ջ����������ˡ�}������˵���ҵ�����ǰһ����ջ�ġ�{����ƥ��ġ�}����
		 * ���ҵ���һ��Ԫ�飬��ô�����һ����ջ�ġ�{��������
		 * ÿ�ε����꣬���brk�Ƿ�Ϊ�ա������Ϊ�գ�����������һ���ַ���
		 * ���Ϊ�գ���ǰ������strJson���ַ��ɱ�����һ��json���ݣ�����
		 * ����liStr���������strJson��������һ�ν�����
		 * 
		 * ��������liStr������ÿһ��Ԫ��Ϊ��������json����
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
