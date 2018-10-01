import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

public class UserData {
    private UserData() throws IOException {
        loadInfo(userDataSrc);
    }
    static String userDataSrc = "";
    static UserData userData;
    public static void setUserDataSrc(String src){
        userDataSrc = src;
    }

    public static UserData getInstance() throws IOException {
        if (userData == null) {
            userData = new UserData();
        }
        return userData;
    }

    static HashMap<String, String> idInfoMap = new HashMap<>();
    static HashMap<String, String> idNameMap = new HashMap<>();

    public String idToInfo(String id) {
        return idInfoMap.get(id);
    }

    public String idToFullName(String id) {
        return idNameMap.get(id);
    }

    public String idToFisrtName(String id) {
        return idInfoMap.get(id).split(",")[0];
    }

    public String idToLastName(String id) {
        return idInfoMap.get(id).split(",")[1];
    }

    public String idToAddress(String id) {
        return idInfoMap.get(id).split(",")[2];
    }

    public String idToCity(String id) {
        return idInfoMap.get(id).split(",")[3];
    }

    public String idToState(String id) {
        return idInfoMap.get(id).split(",")[4];
    }

    public String idToZipcode(String id) {
        return idInfoMap.get(id).split(",")[5];
    }

    public String idToCountry(String id) {
        return idInfoMap.get(id).split(",")[6];
    }

    public String idToUsername(String id) {
        return idInfoMap.get(id).split(",")[7];
    }

    public String idToDob(String id) {
        return idInfoMap.get(id).split(",")[8];
    }

    private void loadInfo(String filePath) throws IOException {
        // format:
        // id,firstname,lastname,address,city,state,zipcode,country,username,dob
        // 0,Evangeline,Taylor,3396 Rogers Street,Loveland,Ohio,45140,US,Unfue1996,1/24/1996
        URI uri;
        try {
            uri = new URI(filePath);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return;
        }
        FileSystem fs = FileSystem.get(uri, new Configuration());
        InputStream in = null;
        try {
            in = fs.open(new Path(filePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = "";
            // Read each line, and load to HashMap
            while ((line = br.readLine()) != null) {
                String fields[] = line.split(",");
                if (fields.length >= 2) {
                    String id = fields[0];
                    String fullname = fields[1] + " " + fields[2];
                    ArrayList<String> infoFields = new ArrayList<>();
                    for (int i = 1; i < fields.length; i++) {
                        infoFields.add(fields[i]);
                    }
                    idInfoMap.put(id, String.join(",", infoFields));
                    idNameMap.put(id, fullname);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }


}
