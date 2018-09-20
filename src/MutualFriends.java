import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MutualFriends {
    public static class MutualFriendsMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input format:
            //  key:docid
            //  value:id<TAB>id1,id2...
            String strId = value.toString().split("\t")[0].trim();
            int intId = Integer.parseInt(strId);
            String[] arrFriends = value.toString().split("\t")[1].trim().split(",");
            ArrayList<String> listFriends = new ArrayList<String>(Arrays.asList(arrFriends));
            for (int i = 0; i < listFriends.size(); i++) {
                String strFriend = listFriends.get(i);
                // indicate id's place in emitted key-value pair, smaller one is placed in front
                int place = (intId < Integer.parseInt(strFriend) ? 0 : 1);
                // format: id1-id2
                String newKey = (place == 0 ? (strId + "," + strFriend) : (strFriend + "," + strId));
                // process potential friends list
                ArrayList<String> listPotentialFriends=new ArrayList<>(listFriends);
                listPotentialFriends.remove(i);
                String strFriendsList = String.join(" ",listPotentialFriends);
                // format: id1 id2..,- or -,id1 id2..
                // ids are friends of respective position's id, '-' means ignorance
                String newValue = (place == 0 ? (strFriendsList + ",-") : ("-," + strFriendsList));
                // emit key-value pair
                context.write(new Text(newKey), new Text(newValue));
            }
        }
    }

    public static class MutualFriendsReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input format:
            //  key:    id1-id2
            //  value:  id1 id2..,- or -,id1 id2..

            HashSet<String> setFriends1 = new HashSet<>(); // friends of id1
            HashSet<String> setFriends2 = new HashSet<>(); // friends of id2
            ArrayList<String> mutualFriends = new ArrayList<>(); // mutual friends
            for (Text value:values){
                String friends;
                int position;
                if (value.toString().split(",")[0].equals("-")){
                    position=1;
                    friends = value.toString().split(",")[1];
                }else {
                    position=2;
                    friends = value.toString().split(",")[0];
                }
                String[] arrFriends = friends.split(" ");
                for (String strFriend:arrFriends){
                    if (position==1)
                        setFriends1.add(strFriend);
                    else
                        setFriends2.add(strFriend);
                }
                // join
                for (String potentialFriend:setFriends1){
                    if (setFriends2.contains(potentialFriend)){
                        mutualFriends.add(potentialFriend);
                    }
                }
            }
            // output format:
            //  key:    id1-id2
            //  value:  id1,id2,...
            Text newValue=new Text(String.join(",",mutualFriends));
            context.write(key,newValue);
        }
    }

}
