import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RatingMatrix {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //inputValue: userId,movie,rating
            //outputKey: userId   outputValue: movie : rating
            String[] line = value.toString().split(",");
            context.write(new Text(line[1]), new Text(line[0]+":"+line[2]));
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //collect all movie ratings and get average rate score for non-rating movie
            //inputKey: userId   inputValue: movie : rating
            //outputKey:userId   outputValue: movie, rating
            double totalscore = 0;
            int totalRatingCount = 0;
            Map<String, Double> movie_rating_map = new HashMap<String, Double>();
            //collect movie_rating
            for(Text value: values){
                String[] movie_rating = value.toString().trim().split(":");
                //if the movie is rated, then put it into map, otherwise, assign rating score to -1 and put it into map
                if(movie_rating.length == 2){
                    double rating = Double.parseDouble(movie_rating[1]);
                    totalscore += rating;
                    totalRatingCount +=1;
                    movie_rating_map.put(movie_rating[0],rating);
                }else{
                    double rating = -1;
                    movie_rating_map.put(movie_rating[0],rating);
                }
            }

            for(Map.Entry<String, Double> entry: movie_rating_map.entrySet()){
                String outputKey = key.toString();
                String outputValue = entry.getKey() + "," +entry.getValue();
                if(entry.getValue() == -1){
                    double average = totalscore/totalRatingCount;
                    outputValue = entry.getKey() + "," +average;
                }
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setJarByClass(RatingMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
