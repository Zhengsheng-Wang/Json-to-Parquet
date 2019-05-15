import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.Repeated;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

public class Writer{
	/*
	 * @param strSchema the string representation of schema
	 * @param strOutPath the output file hdfs
	 */
	private static Group group;
	public static void writeParquet(String strJson, String strSchema, String strOutPath) throws IOException{
		Path path = new Path(strOutPath);
		
		MessageType schema = MessageTypeParser.parseMessageType(strSchema);
		GroupFactory factory = new SimpleGroupFactory(schema);
		Configuration configuration = new Configuration();
		ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path).withConf(configuration).withType(schema);
		
		ParquetWriter<Group> writer = builder.build();
		group = factory.newGroup();
		writer.write(group);
		writer.close();
	}
}
