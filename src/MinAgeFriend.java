import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;

public class MinAgeFriend {
    public static class FriendsRecordMapper extends Mapper<Object,Text,IntWritable,Text>{
        UserData userData;
        @Override
        protected void setup(Context context) throws IOException{
            userData = UserData.getInstance();
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length<2)
                return;
            String id = fields[0];
            String[] friends = fields[1].split(",");
            ArrayList<Integer> ages = new ArrayList<>();
            for (String friend:friends){
                String dob = userData.idToDob(friend);
                // format:1/24/1996
                int year = Integer.parseInt(dob.split("/")[2]);
                int age = Calendar.getInstance().get(Calendar.YEAR) - year;
                ages.add(age);
            }
            int minAge = Collections.min(ages);
            // output:(reverse key and value)
            // key:min age
            // value:id
            context.write(new IntWritable(minAge),new Text(id));
        }
    }

    public static class MinAgeFriendReducer extends Reducer<IntWritable,Text,Text,Text>{
        UserData userData;
        int count = 0;
        int topN;
        @Override
        protected void setup(Context context) throws IOException{
            userData = UserData.getInstance();
            topN = context.getConfiguration().getInt("topN",10);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (count<topN) {
                for (Text value : values) {
                    String address = userData.idToAddress(value.toString());
                    String city = userData.idToCity(value.toString());
                    String state = userData.idToState(value.toString());
                    String fullAddress = address + ", " + city + ", " + state;
                    context.write(new Text(value), new Text(fullAddress + ", " + key.toString()));
                    count++;
                    if (count>=topN)
                        break;
                }
            }
        }
    }
    public static class IntComparator extends WritableComparator {
        public IntComparator() {
            super(IntWritable.class);
        }
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }
    public static void main(String[] args) throws Exception {
        String input = MutualFriends.friendsRecordsInputFile, output = "hdfs://localhost:9000/user/bryan/output/hw1/min_age_friends";
        if (args.length==2){
            input = args[0];
            output = args[1];
        }
        Job job = Job.getInstance();
        job.setJarByClass(MinAgeFriend.class);
        Configuration conf = job.getConfiguration();
        conf.setInt("topN",10);
        // Setup MapReduce
        job.setMapperClass(FriendsRecordMapper.class);
        job.setReducerClass(MinAgeFriendReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(IntComparator.class);
        // Input
        FileInputFormat.addInputPath(job, new Path(input));
        // Output
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
