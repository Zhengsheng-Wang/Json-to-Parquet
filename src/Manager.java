import java.util.LinkedList;
import java.util.List;

import java.io.IOException;
import java.net.URISyntaxException;

public class Manager {
	public static void main(String args[]) throws IOException, URISyntaxException{
		/*
		 *args[0]�ǰ�������json���ݵ��ļ�����ȻJsonFormater.format()���Դ���һ���ļ��������
		 *json���ݵ�������������ǲ��Ե�ʱ��
		*/
		List<String> liJson = JsonFormater.format(args[0]);
		String strOutPath = args[1] + args[0].substring(args[0].lastIndexOf("\\") + 1, args[0].lastIndexOf("."))
				+ ".parq";

		JsonFactory jsonFactory = new JsonFactory();
		SchemaBuilder schemaBuilder = new SchemaBuilder();
		schemaBuilder.jsonFactory = jsonFactory;

		LinkedList<JsonFactory.JsonObject> liJsonObj = new LinkedList<>();
		LinkedList<String> liStrSchema = new LinkedList<>();
		for(int i = 0; i != liJson.size(); ++i){
			jsonFactory.run(liJson.get(i));
			liJsonObj.add(JsonFactory.jsonObj);
			
			schemaBuilder.transform(JsonFactory.jsonObj);
			liStrSchema.add(schemaBuilder.strSchema.toString());
		}
		
		Writer writer = new Writer();
		writer.write(liStrSchema, liJsonObj, strOutPath);
		return;
	}
}
