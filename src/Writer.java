import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.create_database_args;
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
	public void write(String strSchema, JsonFactory.JsonObject jsonObj, String strOutPath)throws IOException{
		Path path = new Path(strOutPath);

		MessageType schema = MessageTypeParser.parseMessageType(strSchema);

		GroupFactory factory = new SimpleGroupFactory(schema);
		Configuration configuration = new Configuration();

		ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path).withConf(configuration).withType(schema);
		ParquetWriter<Group> writer = builder.build();
		
		Group grp = factory.newGroup();
		writeGroup(grp, jsonObj);
		writer.write(grp);
		writer.close();
	}

	private void writeArray(Group grp, JsonFactory.JsonArray jsonArr, String strName){
		for(Object obj : jsonArr.liMem){
			if(obj instanceof JsonFactory.JsonObject){
				Group grpNew = grp.addGroup(strName);
				writeGroup(grpNew, (JsonFactory.JsonObject)obj);
			}
			else if(obj instanceof JsonFactory.JsonArray){
				writeArray(grp, (JsonFactory.JsonArray)obj, strName);
			}
			else{
				if(obj == null){
					continue;
				}
				else{
					if(obj instanceof Integer){
						grp.append(strName, (Integer)obj);
					}
					else if(obj instanceof Float){
						grp.append(strName, (Float)obj);
					}
					else if(obj instanceof String){
						grp.append(strName, (String)obj);
					}
					else if(obj instanceof Boolean){
						grp.append(strName, (Boolean)obj);
					}
				}
			}
		}
	}
	private void writeGroup(Group grp, JsonFactory.JsonObject jsonObj){
		for(JsonFactory.JsonElement jsonEle : jsonObj.liMem){
			String strName = (String)jsonEle.liPath.getLast();

			switch (jsonEle.strType) {
			case "group":
				Group grpNew = grp.addGroup(strName);
				writeGroup(grpNew, (JsonFactory.JsonObject)jsonEle.objVal);
				break;
			case "repeated":
				writeArray(grp, (JsonFactory.JsonArray)jsonEle.objVal, strName);
				break;
			case "INT32":
				grp.append(strName, (Integer)jsonEle.objVal);
				break;
			case "FLOAT":
				grp.append(strName, (Float)jsonEle.objVal);
				break;
			case "BOOLEAN":
				grp.append(strName, (Boolean)jsonEle.objVal);
				break;
			case "BINARY":
				grp.append(strName, (String)jsonEle.objVal);
				break;
			default:
				break;
			}
		}
	}
}