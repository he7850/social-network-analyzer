
import com.sun.istack.NotNull;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;

public class MutualFriends {

    static void parsePotentialFriends(@NotNull String value, @NotNull ArrayList<String> friendsList0, @NotNull ArrayList<String> friendsList1) {
        //  value:  0:id1,id2.. or 1:id1,id2..
        if (value.split(":").length < 2)
            // ignore the pair who have no potential friends
            return;
        String index = value.split(":")[0];
        String[] friends = value.split(":")[1].split(",");
        if (index.equals("0")) {
            friendsList0.addAll(Arrays.asList(friends));
        }
        if (index.equals("1")) {
            friendsList1.addAll(Arrays.asList(friends));
        }
    }

    static ArrayList<String> findMutualFriends(ArrayList<String> friendsList0, ArrayList<String> friendsList1) {
        ArrayList<String> mutualFriends = new ArrayList<>(); // mutual friends
        for (String potentialFriend0 : friendsList0) {
            for (String potentialFriend1 : friendsList0) {
                if (potentialFriend0.equals(potentialFriend1)) {
                    mutualFriends.add(potentialFriend0);
                }
            }
        }
        return mutualFriends;
    }


    public static class MutualFriendsMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) {
            try {
                // input format:
                //  key:docid
                //  value:id<TAB>id1,id2...
                if (value.toString().split("\t").length < 2)
                    // ignore who has no friend
                    return;
                String id = value.toString().split("\t")[0];
                ArrayList<String> friendsList = new ArrayList<>(Arrays.asList(value.toString().split("\t")[1].split(",")));
                for (int i = 0; i < friendsList.size(); i++) {
                    // output potential friends of id and friendsList[i]
                    String friend = friendsList.get(i);
                    // process potential friends list
                    ArrayList<String> potentialFriendsList = new ArrayList<>(friendsList);
                    potentialFriendsList.remove(i);
                    String potentialFriends = String.join(",", potentialFriendsList);
                    // format: 0:id1,id2.. or 1:id1,id2..
                    // ids are potential friends from 0th or 1st id
                    // format: id1,id2
                    String newKey = (Integer.parseInt(id) < Integer.parseInt(friend) ? (id + "," + friend) : (friend + "," + id));
                    String newValue = (Integer.parseInt(id) < Integer.parseInt(friend) ? "0:" : "1:") + potentialFriends;
                    // emit key-value pair
                    context.write(new Text(newKey), new Text(newValue));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                System.err.println("value:" + value);
            }
        }
    }


    public static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {

                // input format:
                //  key:    id1,id2
                //  value:  0:id1,id2.. or 1:id1,id2..
                ArrayList<String> friendsList0 = new ArrayList<>(); // friends of id1
                ArrayList<String> friendsList1 = new ArrayList<>(); // friends of id2
                // combine friends list from the same id
                for (Text value : values) {
                    parsePotentialFriends(value.toString(), friendsList0, friendsList1);
                }
                // find mutual friends
                ArrayList<String> mutualFriends = findMutualFriends(friendsList0, friendsList1);
                // output format:
                //  key:    id1,id2
                //  value:  id1,id2,...
                if (mutualFriends.size() == 0)
                    return;
                Text newValue = new Text(String.join(",", mutualFriends));
                context.write(key, newValue);


            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                for (Text value : values) {
                    System.err.println("values:" + value);
                }
            }
        }


    }

    public final static String friendsRecordsInputFile = "hdfs://localhost:9000/user/bryan/input/hw1/soc-LiveJournal1Adj.txt";
    public final static String mutualFriendsOutputDir = "hdfs://localhost:9000/user/bryan/output/hw1/mutual_friends";

    public static void main(String[] args) throws Exception {
        // do clean: hdfs dfs -rm -R output/hw1/mutual_friends
        String input = friendsRecordsInputFile, output = mutualFriendsOutputDir;
        if (args.length==2){
            input = args[0];
            output = args[1];
        }

        Job job = Job.getInstance();
        job.setJobName("mutual friends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(MutualFriendsMapper.class);
        job.setReducerClass(MutualFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
