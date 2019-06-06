import java.util.List;

import java.io.IOException;
import java.net.URISyntaxException;

public class Manager {
	public static void main(String args[]) throws IOException, URISyntaxException{
		/*
		 *args[0]是包含单个json数据的文件，虽然JsonFormater.format()可以处理一个文件包含多个
		 *json数据的情况，但是我们测试的时候
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
