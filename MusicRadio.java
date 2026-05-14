// package org.apache.hadoop.examples;
// //Complete Code for Music Dataset — Radio Listens & Skips
// Sample Input File (music.txt)
// trackId  radio  skipped
// track1   1      0
// track1   1      1
// track2   0      0
// track2   1      1
// track3   1      0
// track3   1      1
// track3   0      1

// radio=1 means listened on radio, skipped=1 means track was skipped.
import java.io.IOException;
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

public class MusicRadio {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text trackKey = new Text();
    private Text infoValue = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String line = value.toString().trim();

      // skip header line and empty lines
      if (line.startsWith("trackId") || line.isEmpty()) return;

      String[] parts = line.split("\\s+");
      if (parts.length == 3) {
        String trackId = parts[0];   // e.g. track1
        String radio   = parts[1];   // 1 = listened on radio
        String skipped = parts[2];   // 1 = skipped

        trackKey.set(trackId);
        infoValue.set(radio + "," + skipped);   // e.g. "1,0"
        context.write(trackKey, infoValue);     // emits (track1, "1,0")
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int totalRadio   = 0;
      int totalSkipped = 0;

      for (Text val : values) {
        String[] parts = val.toString().split(",");
        int radio   = Integer.parseInt(parts[0]);  // 0 or 1
        int skipped = Integer.parseInt(parts[1]);  // 0 or 1

        totalRadio   += radio;    // count radio listens
        totalSkipped += skipped;  // count skips
      }

      // output: track1   RadioListens:2   Skipped:1
      String output = "RadioListens:" + totalRadio
                    + "   Skipped:" + totalSkipped;
      context.write(key, new Text(output));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: musicradio <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "music radio analysis");
    job.setJarByClass(MusicRadio.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}