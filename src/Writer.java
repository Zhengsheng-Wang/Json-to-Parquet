import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

/*
 * Writer类定义了将json数据写入hadoop dfs的parquet文件的，写入器
 * Writer实例调用write函数，根据传入的schema字符串strScheme，json对象jsonObj，输出文件的路径strOutPath，将
 * jsonObj表示的json对象写入strOutPath指示的文件中
 */
public class Writer{
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

	/*
	 * writeArray方法将键值为strName的数组jsonArr写入grp组内
	 * 迭代jsonArr数组内的元素，如果元素类型为NULL，跳过；如果元素类型为json对象，转到writeObject方法
	 */
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
	/*
	 * writeGroup方法将json对象jsonObj写入专门为该对象建立的组grp内
	 * 直接跳过类型为NULL的键值对
	 */
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