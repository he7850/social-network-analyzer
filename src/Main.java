import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestJobCounters;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("usage:MutualFriends <in> <out>");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "find mutual friends");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TestJobCounters.TokenizerMapper.class);
        job.setCombinerClass(TestJobCounters.IntSumReducer.class);
        job.setReducerClass(TestJobCounters.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
