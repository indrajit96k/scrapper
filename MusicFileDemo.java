
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MusicFileDemo {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text trackkey = new Text();
    private Text infoValue = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] parts=value.toString().split(" ");
      if (parts.length==3) {
    	  trackkey.set(parts[0]);
    	  infoValue.set(parts[1]+","+parts[2]);
        
        context.write(trackkey, infoValue);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	HashSet<String> uniq=new HashSet<>();
      int totalshares = 0;
      for (Text val : values) {
    	  String[] parts=val.toString().split(",");
    	  uniq.add(parts[0]);
        totalshares += Integer.parseInt(parts[1]);
      }
      String output="UniqueListeners" + uniq.size() + "TotalShares:" + totalshares; 
      context.write(key, new Text(output));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Music file");
    job.setJarByClass(MusicFileDemo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
