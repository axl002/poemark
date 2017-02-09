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

    static KafkaProducer<String, String> producer;
    static int reps = 0;
    static int numberOfQueryToGet = 5000;
    static String startingKey = "0";
    static String theSource = "http://api.pathofexile.com/public-stash-tabs?id=";
    static String whereToDump = "testdump/";
    static String topic = "poe2";
    static String currentKey;
    static String pathToScrapedLogs = "/home/ubuntu/hugedump/";

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
        // THIS WORKS NOW
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("metadata.broker.list", "broker1:9092,broker2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 20000000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        // change this to a time stamp serializer
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

        // for item serialization
        //props.put("value.serializer", ItemSerializer.class.getName());
        //props.put("value.serializer.jackson.smile", "true");

        //producer = new KafkaProducer<String, Item>(props);

        currentKey = startingKey;
        int howMany = 0;
        while( howMany < numberOfQueryToGet){
            try {
//                File folder = new File("/home/ubuntu/hugedump");
//                File[] listOfFiles = folder.listFiles();
//
//                for(int k = 0; k < listOfFiles.length; k++){
//                    //File file = listOfFiles[k];
//                    if( listOfFiles[k].getName().contains(".json"))
//                    {
//                        goProduce(currentKey, pullLocalJson(listOfFiles[k]));
//
//                        //System.out.println(howMany < numberOfQueryToGet);
//                    }
//                }

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

        //URL url = new URL("http://api.pathofexile.com/public-stash-tabs?id=0");
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
            String accountName = currentStash.get("accountName").asText();
            String stashId = currentStash.get("id").asText();
            //System.out.println(accountName);
            String stashName = currentStash.get("stash").asText();
            JsonNode itemArray = currentStash.get("items");
            //System.out.println(itemArray.toString());
            if(itemArray != null){

                Iterator<JsonNode> itemIt = itemArray.iterator();
                while(itemIt.hasNext()){

                    JsonNode currentItem = itemIt.next();

                    ObjectNode on = currentItem.deepCopy();
                    on.put("accountName", currentStash.get("accountName").asText());
                    on.put("stashID", currentStash.get("id").asText());
                    on.put("stashName",stashName);
//                    try {
//                        String priceNote = currentItem.get("note").asText();
//                        Item item = new Item();
//                        item.setY(currentItem.get("y").asText());
//                        item.setX(currentItem.get("x").asText());
//                        item.setIdentified(currentItem.get("identified").asBoolean());
//                        item.setItemID(currentItem.get("id").asText());
//                        item.setName(currentItem.get("name").asText());
//                        item.setTypeLine(currentItem.get("typeLine").asText());
//                        //System.out.println(priceNote);
//                        item.setNote(priceNote);
//
//
//                        item.setLastSeller(accountName);
//                        item.setLastStashID(stashId);
//                        item.setLeague(currentItem.get("league").asText());
//                        item.setIlvl(currentItem.get("ilvl").asInt());
//                        try{
//                            item.setExplicitMods(currentItem.get("explicitMods").toString());
//                        }catch (NullPointerException noExpMods){
//                            // not all items have explicit mods
//                        }
                    //System.out.println(on.get("stashID").asText());

                    // test code for pushing string data
                    DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
                    Date dateobj = new Date();
                    //producer.send(new ProducerRecord<String, String>(topic, df.format(dateobj)+ nextChangeId, Double.toString(item.getPrice()) ));

                    producer.send(new ProducerRecord<String, String>(topic, df.format(dateobj)+ nextChangeId, on.toString() ));
                    //System.out.println(item.toString());
//                    }catch (NullPointerException oopsNoNote){
//                        //System.out.println("no note, no price, no list");
//                    }
                }
            }
        }

        // check if newIDReceived
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




//System.out.println(" you win " + bigStashArray.iterator().next().asText());



//            JsonParser jp = mapper.getFactory().createParser(new File("/home/user/Documents/apitest/hugedump/0.txt"));
//            JsonToken test = jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
//            if(test!= JsonToken.START_OBJECT){
//                throw new IllegalStateException("Error not at start of json");
//            }
//
//
//
//            while (jp.nextToken() == JsonToken.FIELD_NAME) {
//
//                String fieldname = jp.getCurrentName();
//                System.out.println("first place "+fieldname);
//                //jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
//                if ("next_change_id".equals(fieldname)) { // contains an object
//                    jp.nextToken();
//                    String next_id = jp.getValueAsString();
//                    rf.setNext_change_id(next_id);
//
//                    System.out.println("foooo"+rf.getnext_change_id());
//                } else if ("stashes".equals(fieldname)) {
//
//
//
////
////                    ObjectMapper mapper = new ObjectMapper();
////
////                    List<RawFormat.Stash> stashList = mapper.readValue(jp.getText(), new TypeReference<List<RawFormat.Stash>>(){});
//                    String testing = jp.getValueAsString();
//                    System.out.println("barrrr"+testing);
////                    for (RawFormat.Stash stash: stashList){
////                        System.out.println(stash.getId());
////                    }
////
////                    ObjectMapper objectMapper = new ObjectMapper(); objectMapper.getTypeFactory();
////                    List<SomeClass> someClassList = mapper.readValue(jsonString, typeFactory.constructCollectionType(List.class, SomeClass.class));
////
////                    List<SomeClass> list = mapper.readValue(jsonString, new TypeReference<List<SomeClass>>() { });
////                    SomeClass[] array = mapper.readValue(jsonString, SomeClass[].class);
////                    rf.setStashes(jp.getText());
//
//                }
//throw new IllegalStateException("Unrecognized field '"+fieldname+"'!");
//}
//jp.close(); // ensure resources get cleaned up timely and properly
//
// System.out.println(rf.getnext_change_id());

