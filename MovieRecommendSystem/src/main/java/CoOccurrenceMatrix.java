import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

//generate non-normalized co-occurrence matrix
public class CoOccurrenceMatrix {
    //generate uer-movie table
    public static class GenerateMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //InputValue = userId \t <movieId1: rating1, movieId2: rating2, ...>
            //Output Key = movie1 : movie2   Output Value = 1
            String line = value.toString().trim();
            String[] user_movie_ratings = line.split("\t");
            //if some user didn't rate any movie
            if(user_movie_ratings.length != 2){
                //logger.info("Found user without movie");
                return;
            }

            String[] movie_ratings = user_movie_ratings[1].split(",");
            for(int i = 0; i < movie_ratings.length; i++){
                String movie1 = movie_ratings[i].trim().split(":")[0];
                for(int j = 0; j < movie_ratings.length; j++){
                    String movie2 = movie_ratings[j].trim().split(":")[0];
                    String outputKey = movie1 + ":" + movie2;
                    context.write(new Text(outputKey), new IntWritable(1));
                }
            }
        }
    }

    public static class GenerateMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            //InputKey = movie1 : movie2   InputValue = <1, 1, 1, 1>
            //OutputKey = movie1 : movie2  OutputValue = sum
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(GenerateMatrixMapper.class);
        job.setReducerClass(GenerateMatrixReducer.class);

        job.setJarByClass(CoOccurrenceMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
