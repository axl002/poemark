import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.*;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JackKafka {

    private static KafkaProducer<String, String> producer;
    private static int reps = 0;
    private static int numberOfQueryToGet = 5000;
    private static String startingKey = "0";
    private static String theSource = "http://api.pathofexile.com/public-stash-tabs?id=";
    private static String whereToDump = "testdump/";
    private static String topic = "poe2";
    private static String currentKey;
    //static String pathToScrapedLogs = "/home/ubuntu/hugedump/";

    public static void main(String[] args) {

        // for running with args
        if(args.length == 3){
            startingKey = args[0];
            numberOfQueryToGet = Integer.parseInt(args[1]);
            whereToDump = args[2];
        }
        if (args.length ==1){
            startingKey = args[0];
        }

        //kafka config
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 20000000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

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
        producer.close();
    }

    private static void goProduce(String keyToUse, String content) throws NullPointerException, InterruptedException, IOException{

        //create object mapper load file to parse
        ObjectMapper mapper = new ObjectMapper();

        URL url = new URL(theSource + keyToUse);

        content = pullURL(url);
        JsonNode rootNode = null;
        if(!isContentNull(content)){
            rootNode = mapper.readTree(content);
            //writeOutput(whereToDump,keyToUse,content);
        }

        // extract next change id
        String nextChangeId = rootNode.get("next_change_id").asText();

        // make array of stashes
        JsonNode bigStashArray = rootNode.get("stashes");

        // iterate through and send each item in each stash
        Iterator<JsonNode > bigIt = bigStashArray.iterator();
        while( bigIt.hasNext()){

            JsonNode currentStash = bigIt.next();
            String stashName = currentStash.get("stash").asText();
            JsonNode itemArray = currentStash.get("items");
            if(itemArray != null){

                Iterator<JsonNode> itemIt = itemArray.iterator();
                while(itemIt.hasNext()){

                    JsonNode currentItem = itemIt.next();

                    ObjectNode on = currentItem.deepCopy();
                    on.put("accountName", currentStash.get("accountName").asText());
                    on.put("stashID", currentStash.get("id").asText());
                    on.put("stashName",stashName);

                    DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
                    Date dateobj = new Date();

                    producer.send(new ProducerRecord<String, String>(topic, df.format(dateobj)+ nextChangeId, on.toString() ));

                }
            }
        }

        // check if newIDReceived
        checkNew(nextChangeId);
    }

    // write json to disk
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

    // read local json
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