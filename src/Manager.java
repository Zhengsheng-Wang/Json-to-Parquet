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
		jsonFactory.run(liJson.get(0));
		JsonFactory.JsonObject jsonObj = JsonFactory.jsonObj;
		
		SchemaBuilder schemaBuilder = new SchemaBuilder();
		schemaBuilder.transform(jsonObj);
		StringBuilder strSchema = SchemaBuilder.strSchema;
		
		Writer writer = new Writer();

		writer.write(strSchema.toString(), jsonObj, strOutPath);
		return;
	}
}
