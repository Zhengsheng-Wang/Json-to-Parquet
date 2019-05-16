import java.util.List;
import java.util.LinkedList;
import java.io.IOException;
import java.net.URISyntaxException;

public class Manager {
	public static void main(String args[]) throws IOException, URISyntaxException{
		List<String> liJson = JsonFomater.Format(args[0]);
		String strOutPath = args[1];

		//Writer.writeParquet(liJson.get(0), "", strOutPath);
		JsonFactory jsonFactory = new JsonFactory();
		jsonFactory.run(liJson.get(0));
		JsonFactory.JsonObject jsonObj = jsonFactory.jsonObj;
		
		return;
	}
}
