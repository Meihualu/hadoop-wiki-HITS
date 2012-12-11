package fudan.HITS.job1.linkin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class FromPagesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
    	int pageTabIndex = value.find("\t");
        String page = Text.decode(value.getBytes(), 0, pageTabIndex);        
        String links = Text.decode(value.getBytes(), pageTabIndex+1, value.getLength()-(pageTabIndex+1));
        String[] allOtherPages = links.split(",");
        
        // Mark page as an Existing page (ignore red wiki-links)
        output.collect(new Text(page), new Text("!"));
        
        for (String otherPage : allOtherPages){
            output.collect(new Text(otherPage), new Text(page));
        }
    }
}