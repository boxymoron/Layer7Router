import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

public class Lambda {

	//some mock classes
	static class Logger {
		public void error(String err, Exception e){System.err.println(err);}
		public void error(String err){System.err.println(err);}
	}
	static class ResourceResolver{}
	static class LinkUtils{
		public static String formatLink(String path, ResourceResolver rr){
			return path;
		}
	}
	
	public static void main(String[] args){
		//Some mock objects
		Logger logger = new Logger();
		ResourceResolver resourceResolver = new ResourceResolver();
		
		final List<String> jsonList = new ArrayList<>();
		jsonList.add("{title: \"title1\", path=\"/a/b/c\", target=\"_blank\"}");//good
		jsonList.add("{title: \"title2\", path=\"/c/d/e\", target=\"_blank\"}");//good
		jsonList.add("{missingtitle: \"title3\", value={b: \"b\"}}");//bad
		jsonList.add("{title: \"title4\", missingvalue={b: \"b\"}}");//bad
		jsonList.add("{badJson}");//bad

		final List<Map<String, String>> mapStr = jsonList.stream()
				.flatMap(jsonChunk -> {
					try {
						return Stream.of(new JSONObject(jsonChunk));
					} catch (JSONException e) {
						logger.error("Error parsing JSON: "+jsonChunk, e);
						return null;
					}
				})
				.filter(jsonObject -> {
					try{
						if("".equals(jsonObject.optString("title"))){//check that it has a title
							System.err.println("Missing key 'title' for JSON: "+jsonObject.toString(4));
							return false;
						}
						if("".equals(jsonObject.optString("path"))){//check that it has a path
							System.err.println("Missing key 'path' for JSON: "+jsonObject.toString(4));
							return false;
						}
						if("".equals(jsonObject.optString("target"))){//check that it has a target
							logger.error("Missing key 'target' for JSON: "+jsonObject.toString(4));
							return false;
						}
						return true;
					}catch(JSONException e){
						e.printStackTrace();
						return false;
					}
				})
				.flatMap(jsonObject -> {
					try{
						final Map<String, String> map = new HashMap<>();
						map.put("title", jsonObject.getString("title"));
						String path = jsonObject.getString("path");
		                path = LinkUtils.formatLink(path, resourceResolver);
		                map.put("path", path);
		                map.put("target", jsonObject.getString("target"));
						return Stream.of(map);
					}catch(JSONException e){
						logger.error("", e);
						return null;
					}
				})
				.collect(Collectors.toList());

		mapStr.forEach((v)->{
			System.out.println(v);
		});
	}
}
