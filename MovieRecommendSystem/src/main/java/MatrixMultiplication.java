import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MatrixMultiplication {

    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            //InputValue = movie2 \t movie1 = relativeRelation
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text>{
        //pass data to reducer
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //inputValue: userId \t movie,rating
            //outputKey: movie   outputValue: userId : rating
            String[] line = value.toString().split("\t");
            String outputKey = line.toString().trim().split(",")[0];
            String outputValue = line[0] + ":" + line.toString().trim().split(",")[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable>{
        //collect the data for each movie, then do the multiplication
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //inputKey = movie2 inputValue = <movie1=relation, movie3=relation... userA:rating, userB:rating...>
            //outputKey = movie : userId
            //outputValue = subRating # relativeRelation * rating

            //map1<movie1, relation>          map2<userA, rating>
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            for(Text value: values){
                if(value.toString().contains("=")){
                    String[] movie_relation = value.toString().split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                }else{
                    String[] user_rating = value.toString().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for(Map.Entry<String, Double> entry: relationMap.entrySet()){
                String movie1 = entry.getKey();
                double relativeRelation = entry.getValue();

                for(Map.Entry<String, Double> entry1: ratingMap.entrySet()){
                    String userId = entry1.getKey();
                    double rating = entry1.getValue();

                    String outputKey = movie1 + ":" + userId;
                    double outputValue = relativeRelation * rating;

                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixMultiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
