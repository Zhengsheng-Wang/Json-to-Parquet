import java.util.List;
import java.io.IOException;
import java.net.URISyntaxException;

public class Manager {
	public static void main(String args[]) throws IOException, URISyntaxException{
		List<String> liJson = JsonFomater.Format(args[0]);
		String strOutPath = args[1] + args[0].substring(args[0].lastIndexOf("\\") + 1, args[0].lastIndexOf("."))
				+ ".parq";

		JsonFactory jsonFactory = new JsonFactory();
		jsonFactory.run(liJson.get(0));
		JsonFactory.JsonObject jsonObj = JsonFactory.jsonObj;
		
		SchemaBuilder schemaBuilder = new SchemaBuilder();
		schemaBuilder.transform(jsonObj);
		StringBuilder strSchema = SchemaBuilder.strSchema;
		
		/*
		String strSchema1 = "message pair{\n" + 
			  "repeated group course{\n"
			+ "optional binary coursename(UTF8);\n"
			+ "required binary year(UTF8);\n"
			+ "required int32 grade;\n"
			+ "repeated int32 my;\n"
			+ "}\n" + "}";
		*/
		Writer writer = new Writer();

		writer.write(strSchema.toString(), jsonObj, strOutPath);
		return;
	}
}
