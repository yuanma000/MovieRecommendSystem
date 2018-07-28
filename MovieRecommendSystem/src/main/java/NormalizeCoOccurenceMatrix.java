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
import java.util.HashMap;
import java.util.Map;

public class NormalizeCoOccurenceMatrix {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text>{
        //collect the relationship list for movie1
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //InputValue = movie1 : movie2 /t sum
            //OutputKey = movie1   OutputValue = movie2 = sum
            String[] movie_relation = value.toString().trim().split("\t");
            if(movie_relation.length != 2){
                return; // should throw exception
            }
            String movie1 = movie_relation[0].split(":")[0];
            String movie2 = movie_relation[0].split(":")[1];
            String sum = movie_relation[1];
            context.write(new Text(movie1), new Text(movie2 + "=" + sum));
        }
    }

    public static class NormalizeReducer extends  Reducer<Text, Text, Text, Text>{
        //collect all relations to get denominator and iterate all movies to set relation / denominator
        //normalize each unit of co-occurence matrix
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //InputKey = movie1   InputValue = <movie2 = sum , movie3 = sum, movie4 = sum ...>
            //OutputKey = movie2  OutputValue= movie1 = sum(RelativeRelation)
            int denominator = 0;
            Map<String, Integer> movie234_relation_map = new HashMap<String, Integer>();
            //collect all movie's sum in values, and store those into hashmap, the sum of those movies sum is
            //the denominator for relative relation
            for(Text value: values){
                String[] movie_relation = value.toString().trim().split("=");
                int relation = Integer.parseInt(movie_relation[1]);
                denominator += relation;
                movie234_relation_map.put(movie_relation[0],relation);
            }
            //shift key,value pair from row/column to column/row. Matrix Multiplication will get benefit from this.
            for(Map.Entry<String, Integer> entry: movie234_relation_map.entrySet()){
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/denominator;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(NormalizeCoOccurenceMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
