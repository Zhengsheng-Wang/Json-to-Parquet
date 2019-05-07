import java.util.List;
import java.util.LinkedList;
import java.io.IOException;

public class Manager {
	public static void main(String args[]) throws IOException{
		List<String> liJson = JsonFomater.format(args[0]);
		
		String strType = "INT";
		switch(strType){
		case "INT":
			System.out.println("INT");
			break;
		default:
			System.out.println("1");
		}
	}
}
