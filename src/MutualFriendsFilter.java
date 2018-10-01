import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;

public class MutualFriendsFilter {


    public static class MutualFriendsFilterMapper extends Mapper<Object, Text, Text, Text> {
        ArrayList<String> searchKeys = new ArrayList<>();
        @Override
        protected void setup(Context context) {
            searchKeys.add("0,1");
            searchKeys.add("20,28193");
            searchKeys.add("1,29826");
            searchKeys.add("6222,19272");
            searchKeys.add("28041,28056");
        }

        @Override
        protected void map(Object key, Text value, Context context) {
            try {
                // format: id1,id2 id1,id2,...
                String[] tokens = value.toString().split("\t");
                // filter by key: id1,id2
                if (searchKeys.contains(tokens[0])) {
                    System.out.println(value);
                    context.write(new Text(tokens[0]), new Text(tokens[1]));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                System.err.println("value:" + value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String input = MutualFriends.mutualFriendsOutputDir, output = "hdfs://localhost:9000/user/bryan/output/hw1/filtered_result";
        if (args.length==2){
            input = args[0];
            output = args[1];
        }
        Job job = Job.getInstance();
        job.setJarByClass(MutualFriendsFilterMapper.class);
        job.setJobName("Mutual Friends Filter");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(MutualFriendsFilterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
