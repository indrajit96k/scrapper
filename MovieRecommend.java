package org.apache.hadoop.examples;
// Complete Code for Movie Dataset — Recommend Movies Based on User Ratings
// Sample Input File (movies.txt)
// movieId  userId  rating
// movie1   user1   4
// movie1   user2   3
// movie1   user3   5
// movie2   user1   2
// movie2   user2   4
// movie2   user3   3
// movie3   user1   5
// movie3   user2   5
// movie3   user3   4
// movie4   user1   1
// movie4   user2   2
// movie4   user3   3

// rating is on a scale of 1 to 5. Movies with average rating above 3.5 are recommended.
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieRecommend {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text movieKey  = new Text();
    private Text rateValue = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String line = value.toString().trim();

      // skip header line and empty lines
      if (line.startsWith("movieId") || line.isEmpty()) return;

      String[] parts = line.split("\\s+");
      if (parts.length == 3) {
        String movieId = parts[0];   // e.g. movie1
        String userId  = parts[1];   // e.g. user1
        String rating  = parts[2];   // e.g. 4

        movieKey.set(movieId);
        rateValue.set(userId + "," + rating);    // e.g. "user1,4"
        context.write(movieKey, rateValue);      // emits (movie1, "user1,4")
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int totalRating = 0;
      int totalCount  = 0;

      for (Text val : values) {
        String[] parts = val.toString().split(",");
        int rating = Integer.parseInt(parts[1]);  // get rating value

        totalRating += rating;   // sum all ratings
        totalCount++;            // count number of ratings
      }

      // calculate average rating
      double avgRating = (double) totalRating / totalCount;

      // round to 2 decimal places
      String avg = String.format("%.2f", avgRating);

      // recommend if average rating is above 3.5
      String recommendation;
      if (avgRating >= 3.5) {
        recommendation = "RECOMMENDED";
      } else {
        recommendation = "NOT RECOMMENDED";
      }

      // output: movie1   AvgRating:4.00   TotalVotes:3   RECOMMENDED
      String output = "AvgRating:" + avg
                    + "   TotalVotes:" + totalCount
                    + "   " + recommendation;
      context.write(key, new Text(output));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: movierecommend <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "movie recommendation");
    job.setJarByClass(MovieRecommend.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}