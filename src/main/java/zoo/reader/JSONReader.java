package zoo.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import com.google.gson.Gson;

public class JSONReader {
	private static final Gson gson = new Gson();

	public QueryStructure readJson (String jsonInString) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader (jsonInString));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return gson.fromJson(gson.newJsonReader(reader), QueryStructure.class);
	}
}
