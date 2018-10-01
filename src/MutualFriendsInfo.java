import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class MutualFriendsInfo {
    public static class JoinInfoMapper extends Mapper<Object, Text, Text, Text> {
        UserData userData;
        String searchKey;
        String[] searchIds;

        @Override
        protected void setup(Context context) throws IOException {
            userData = UserData.getInstance();
            searchKey = context.getConfiguration().get("searchKey");
            if (searchKey == null) {
                throw new IOException("NO SEARCH KEY");
            }
            searchIds = searchKey.split(", ");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                // input format:
                //  key:docid
                //  value:id<TAB>id1,id2...
                if (value.toString().split("\t").length < 2)
                    // ignore who has no friend
                    return;
                String id = value.toString().split("\t")[0];
                // match search key
                if (id.equals(searchIds[0]) || id.equals(searchIds[1])) {
                    String newValue = "";
                    if (id.equals(searchIds[0])) {
                        newValue = "0:" + value.toString().split("\t")[1];
                    } else {
                        newValue = "1:" + value.toString().split("\t")[1];
                    }
                    // output format:
                    // key:   id1, id2
                    // value: 0:id1,id2.. or 1:id1,id2..
                    // ids are potential friends from 0th or 1st id
                    context.write(new Text(searchKey), new Text(newValue));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("key:" + key);
                System.err.println("value:" + value);
            }
        }


    }


    public static class JoinInfoReducer extends Reducer<Text, Text, Text, Text> {
        UserData userData;
        String searchKey;
        String[] searchIds;

        @Override
        protected void setup(Context context) throws IOException {
            userData = UserData.getInstance();
            searchKey = context.getConfiguration().get("searchKey");
            if (searchKey == null) {
                throw new IOException("NO SEARCH KEY");
            }
            searchIds = searchKey.split(", ");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                // input format:
                //  key:    id1, id2
                //  value:  0:id1,id2.. or 1:id1,id2..
                ArrayList<String> friendsList0 = new ArrayList<>(); // friends of id1
                ArrayList<String> friendsList1 = new ArrayList<>(); // friends of id2
                // combine friends list from the same id
                for (Text value : values) {
                    MutualFriends.parsePotentialFriends(value.toString(),friendsList0,friendsList1);
                }
                // find mutual friends
                ArrayList<String> mutualFriends = MutualFriends.findMutualFriends(friendsList0,friendsList1);
                // find infos
                ArrayList<String> friendsInfos = new ArrayList<>();
                for (String friendId : mutualFriends) {
                    String firstName = userData.idToFisrtName(friendId);
                    String state = userData.idToState(friendId);
                    friendsInfos.add(firstName + ": " + state);
                }
                // format:
                // key:id1, id2
                // value:[Evangeline: Ohio, Charlotte: California]
                Text newValue = new Text("[" + String.join(", ", friendsInfos) + "]");
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

    public static void main(String[] args) throws Exception {
        String input = MutualFriends.friendsRecordsInputFile;
        String src = "hdfs://localhost:9000/user/bryan/input/hw1/userdata.txt";
        String output = "hdfs://localhost:9000/user/bryan/output/hw1/mutual_friends_info";
        if (args.length==3){
            input = args[0];
            src = args[1];
            output = args[2];
        }
        UserData.setUserDataSrc(src);
        String searchKey = "26, 28";
        if (args.length == 1) {
            searchKey = args[0];
        }
        Job job = Job.getInstance();
        job.setJobName("Mutual Friends Info Join");
        job.setJarByClass(UserData.class);
        Configuration conf = job.getConfiguration();
        conf.set("searchKey", searchKey);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(JoinInfoMapper.class);
        job.setReducerClass(JoinInfoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

}
