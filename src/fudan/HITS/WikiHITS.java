package fudan.HITS;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import fudan.HITS.XMLParser.XMLInputFormat;
import fudan.HITS.XMLParser.XMLParserMapper;
import fudan.HITS.XMLParser.XMLParserReducer;
import fudan.HITS.job1.linkin.FromPagesMapper;
import fudan.HITS.job1.linkin.FromPagesReducer;
import fudan.HITS.job2.initial.InitAuthHubMapper;
import fudan.HITS.job2.initial.InitAuthHubReducer;
import fudan.HITS.job3.calculate.HITSCalculateMapper;
import fudan.HITS.job3.calculate.HITSCalculateReducer;
import fudan.HITS.job4.result.HITSRankByAuthMapper;
import fudan.HITS.job4.result.HITSRankByHubMapper;

@SuppressWarnings("deprecation")
public class WikiHITS {
	
	private static NumberFormat nf = new DecimalFormat("00");
	private static int iterations = 5;

	/**
	 * @param args
	 */    
    public static void main(String[] args) throws Exception {
        WikiHITS pageHITS = new WikiHITS();
        
        // get outgoing links
        pageHITS.runXmlParsing(args[0], "wiki/linkout");
        
        // get incoming links
        pageHITS.getLinkIn("wiki/linkout", "wiki/linkin");
        
        // initialization
        pageHITS.initialize("wiki/linkout", "wiki/linkin", "wiki/HITS/iter00");
        
        if (args.length == 2)
        	iterations = Integer.parseInt(args[1]);
        int runs = 0;
        for (; runs < iterations; runs++) {
        	pageHITS.runHITSCalculation("wiki/HITS/iter"+nf.format(runs), "wiki/HITS/iter"+nf.format(runs + 1));
        }
        
        pageHITS.runRankOrdering("wiki/HITS/iter"+nf.format(runs), "wiki/HITS/result");
        
    }
    
    public void runXmlParsing(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiHITS.class);
        
        conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
        conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
        
        // Input / Mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(XMLInputFormat.class);
        conf.setMapperClass(XMLParserMapper.class);
        
        // Output / Reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(XMLParserReducer.class);
        
        JobClient.runJob(conf);
    }
    
    public void getLinkIn(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiHITS.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(FromPagesMapper.class);
        conf.setReducerClass(FromPagesReducer.class);
        
        JobClient.runJob(conf);
    }
    
    private void initialize(String inputPath1, String inputPath2, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiHITS.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(conf, new Path(inputPath1));
        FileInputFormat.addInputPath(conf, new Path(inputPath2));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(InitAuthHubMapper.class);
        conf.setReducerClass(InitAuthHubReducer.class);
        
        JobClient.runJob(conf);
    }

    private void runHITSCalculation(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiHITS.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(HITSCalculateMapper.class);
        conf.setReducerClass(HITSCalculateReducer.class);
        
        JobClient.runJob(conf);
    }
    
    private void runRankOrdering(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiHITS.class);
        
        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        
        FileOutputFormat.setOutputPath(conf, new Path(outputPath+"/byAuth"));
        conf.setMapperClass(HITSRankByAuthMapper.class);
        JobClient.runJob(conf);
        
        
        conf = new JobConf(WikiHITS.class);
        
        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        
        FileOutputFormat.setOutputPath(conf, new Path(outputPath+"/byHub"));
        conf.setMapperClass(HITSRankByHubMapper.class);
        JobClient.runJob(conf);
    }
}
