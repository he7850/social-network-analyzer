import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class TopNFriendPair {
    // used in priority queue
    static class FriendsRecord {
        int num;
        String pair;
        String friends;
        public FriendsRecord(String pair, String friends) {
            this.pair = pair;
            this.friends = friends;
            this.num = friends.split(",").length;
        }
    }


    public static class TopNFriendPairMapper extends Mapper<Object, Text, Text, Text> {
        // min heap
        Queue<FriendsRecord> priorityQueue;
        int N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            priorityQueue = new PriorityQueue<FriendsRecord>(new Comparator<FriendsRecord>() {
                public int compare(FriendsRecord a, FriendsRecord b) {
                    return a.num - b.num;
                }
            });
            N = context.getConfiguration().getInt("topN", 10);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // input format: id1,id2 id1,id2,...
                String[] tokens = value.toString().split("\t");
                if (tokens.length > 1) {
                    FriendsRecord friendsRecord = new FriendsRecord(tokens[0], tokens[1]);
                    priorityQueue.offer(friendsRecord);
                    // keep min heap's size <= N, which is, keep N largest records
                    if (priorityQueue.size() > N) {
                        priorityQueue.poll();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                System.err.println("value:" + value);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // output format:
            // key:id1,id2
            // value:id1,id2,...
            for (FriendsRecord friendsRecord : priorityQueue) {
                context.write(new Text(friendsRecord.pair), new Text(friendsRecord.friends));
            }
        }
    }

    public static class TopNFriendPairReducer extends Reducer<Text, Text, Text, IntWritable> {
        Queue<FriendsRecord> priorityQueue;
        int N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // min heap
            priorityQueue = new PriorityQueue<FriendsRecord>(new Comparator<FriendsRecord>() {
                public int compare(FriendsRecord a, FriendsRecord b) {
                    return a.num - b.num;
                }
            });
            N = context.getConfiguration().getInt("topN", 10);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                // input format:
                // key:id1,id2
                // value:id1,id2,...
                for (Text value : values) {
                    FriendsRecord friendsRecord = new FriendsRecord(key.toString(), value.toString());
                    priorityQueue.offer(friendsRecord);
                    if (priorityQueue.size() > N) {
                        priorityQueue.poll();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                for (Text value : values) {
                    System.err.println("values:" + value);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ArrayList<FriendsRecord> friendsRecordList = new ArrayList<>();
            while (!priorityQueue.isEmpty()){
                friendsRecordList.add(priorityQueue.poll());
            }
            Collections.reverse(friendsRecordList);
            // output format:
            // key:id1,id2
            // value:# of friends
            for (FriendsRecord friendsRecord:friendsRecordList) {
                context.write(new Text(friendsRecord.pair), new IntWritable(friendsRecord.num));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String input = MutualFriends.mutualFriendsOutputDir, output = "hdfs://localhost:9000/user/bryan/output/hw1/topN";
        if (args.length==2){
            input = args[0];
            output = args[1];
        }
        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        conf.setInt("topN", 10);
        job.setJarByClass(TopNFriendPair.class);
        job.setJobName("Top 10 Mutual Friends Pair");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(TopNFriendPairMapper.class);
        job.setReducerClass(TopNFriendPairReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
