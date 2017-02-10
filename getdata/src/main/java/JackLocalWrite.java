import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URL;
import java.util.concurrent.TimeUnit;

// write json to disk
public class JackLocalWrite {

    static int reps = 0;
    static int numberOfQueryToGet = 1000;
    static String startingKey = "0";
    static String theSource = "http://api.pathofexile.com/public-stash-tabs?id=";
    static String whereToDump = "testdump/";
    static String topic = "poe2";
    static String currentKey;

    public static void main(String[] args) {

        // for running with args
        if(args.length == 3){
            startingKey = args[0];
            numberOfQueryToGet = Integer.parseInt(args[1]);
            whereToDump = args[2];
        }


        currentKey = startingKey;
        int howMany = 0;
        while( howMany < numberOfQueryToGet){
            try {

                goProduce(currentKey, "");
                howMany++;
            } catch (Exception bad) {
                bad.printStackTrace();
            }

        }
        System.out.println("Last key was: " + currentKey);
    }

    private static void goProduce(String keyToUse, String content) throws NullPointerException, InterruptedException, IOException{

        //create object mapper load file to parse
        ObjectMapper mapper = new ObjectMapper();

        URL url = new URL(theSource + keyToUse);

        content = pullURL(url);
        JsonNode rootNode = null;
        if(!isContentNull(content)){
            rootNode = mapper.readTree(content);
            writeOutput(whereToDump,keyToUse,content);
        }

//      extract next change id
        String nextChangeId = rootNode.get("next_change_id").asText();

        checkNew(nextChangeId);
    }

    private static void writeOutput(String outPath, String name, String content) throws IOException{
        FileWriter fw = new FileWriter(new File(outPath + name + ".json"));
        fw.write(content);
        fw.close();

    }
    private static String pullURL(URL whereToPull) throws IOException, InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        //System.out.println(url.toString());
        BufferedReader br = new BufferedReader(new InputStreamReader(whereToPull.openStream()));
        return br.readLine();
    }


    private static String pullLocalJson(File file) throws IOException {
        BufferedReader br = new BufferedReader((new FileReader(file)));
        return br.readLine();
    }

    private static void upDateKeyToUse(String newKey){
        currentKey = newKey;
    }

    // check for new key id
    private static void checkNew(String newKey) throws InterruptedException{
        if(!newKey.equals(currentKey)) {
            upDateKeyToUse(newKey);
            System.out.println(newKey);
        }
        else {
            // no new key wait
            TimeUnit.SECONDS.sleep(5);
        }
    }

    // check if url call returned null
    private static boolean isContentNull(String content) throws NullPointerException, InterruptedException{
        if(null != content){
            return false;
        }else{
            // pulled null contents
            TimeUnit.SECONDS.sleep(5);
            throw new NullPointerException();
        }
    }
}