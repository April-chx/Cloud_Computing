package workload1;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
	
	Text keyout = new Text();
	Text valueout = new Text();
	
	@SuppressWarnings("unchecked")
	public static Map.Entry[] sortByOrder(Map h) {  
        Set set = h.entrySet();  
        Map.Entry[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);  
        Arrays.sort(entries, new Comparator() {  
            public int compare(Object arg0, Object arg1) {  
                Long key1 = Long.valueOf(((Map.Entry) arg0).getValue().toString());  
                Long key2 = Long.valueOf(((Map.Entry) arg1).getValue().toString());  
                return key2.compareTo(key1);  
               } });
        return entries;  
    }
	
	public void reduce(Text key, Iterable<Text> values, Context context
			) throws IOException, InterruptedException {

		int count=0;
		Map<String, Integer> tagFreq = new HashMap<String,Integer>();
		Map<String, Integer> photoNum = new HashMap<String,Integer>();
		
		for (Text text: values){
			String[] dataArray = text.toString().split("\t");
			String tag = dataArray[0];
			String photo = dataArray[1];

			if (tagFreq.containsKey(tag)){
				tagFreq.put(tag, tagFreq.get(tag) +1);
			}else{
				tagFreq.put(tag, 1);
			}
			
			if (!photoNum.containsKey(photo)){
				photoNum.put(photo, 1);
				count++;
			}
		}

		StringBuffer strBuf = new StringBuffer();

		Map.Entry<String, Integer>[] tag10 = sortByOrder(tagFreq);
		for(int i = 0; i<10&&i<tag10.length; i++){
			strBuf.append(tag10[i].getKey()+":"+tag10[i].getValue()+"\t");
		}
		keyout.set(key.toString());
		valueout.set(count+"\t"+strBuf.toString());
		context.write(keyout, valueout);
	}
}