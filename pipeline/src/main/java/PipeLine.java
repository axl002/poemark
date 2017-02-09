import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;



import javax.annotation.Nullable;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//Special thanks to tgrall for brilliant flink example code and dependency tree
//github: https://github.com/tgrall/kafka-flink-101

// current function 02-01-2017
// filters out duplicates
// filters incoming json to only be from standard league items priced in chaos orbs or exalts
// performs streaming itemcount
// assign price based on fixed lookup

// computer windowed average price in last 6 hours
public class PipeLine{

    public static Properties props;

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        Properties outProps = new Properties();
        // setProperties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "zookeeper");
        props.setProperty("auto.offset.reset","latest");
        outProps.setProperty("bootstrap.servers", "localhost:9092");
        String topic = "poe2";
        String topicOut = "poe3";
        String meanOut = "poe4";

        // part 1 filter out dupes and items we are not considering
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SplitStream<String> stream = env
                .addSource(new FlinkKafkaConsumer09<String>(topic, new SimpleStringSchema(), props))
                .assignTimestampsAndWatermarks(new MyTimeStamp())

                .map(new MapFunction<String, Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public Tuple3<String, ObjectNode, Integer> map(String input) throws Exception {
                        ObjectNode on = parseJsonMutable(input);
                        String key;
                        try {
                            key = on.get("accountName").asText() +
                                    on.get("id").asText() + on.get("note").asText();
                        }catch(NullPointerException npe){
                            key = on.get("accountName").asText() +
                                    on.get("id").asText();
                        }
                        return Tuple3.of(key, on, 1);
                    }
                })
                // keyBy unique key
                .keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(20)))

                .trigger(new Trigger<Tuple3<String, ObjectNode, Integer>, TimeWindow>() {

                    @Override
                    public TriggerResult onElement(Tuple3<String, ObjectNode, Integer> stringObjectNodeIntegerTuple3, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        if(l >= timeWindow.maxTimestamp()){
                            return TriggerResult.FIRE_AND_PURGE;
                        }else{
                            return TriggerResult.CONTINUE;
                        }

                    }
                })
                // aggregate and return count of unique key for de-duping
                .fold(Tuple3.of("0", new ObjectNode(null), 0), new FoldFunction<Tuple3<String, ObjectNode, Integer>, Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public Tuple3<String, ObjectNode, Integer> fold(Tuple3<String, ObjectNode, Integer> old, Tuple3<String, ObjectNode, Integer> current) throws Exception {
                        return Tuple3.of(current.f0, current.f1, current.f2+old.f2);
                    }
                })
                // if the counter of unique id is less than or equal 1, then we have a new item, let it pass
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, ObjectNode, Integer> inputWithDuplicationCounter) throws Exception {
                        return inputWithDuplicationCounter.f2<=1;
                    }
                })
                // other filtering
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, ObjectNode, Integer> input) throws Exception {
                        try {
                            String outString = input.f1.get("note").asText().toLowerCase();
                            return outString.contains("chaos") || outString.contains("exa")
                                    || outString.contains("jeweller") || outString.contains("fusing")
                                    || outString.contains("alchemy");
                        }
                        catch (NullPointerException npe){
                            npe.printStackTrace();
                            return false;
                        }
                    }
                })
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, ObjectNode, Integer> input) throws Exception {
                        String outString = input.f1.get("league").asText();
                        try{
                            return outString.contains("Standard");
                        }
                        catch (NullPointerException npe){
                            npe.printStackTrace();
                            return false;
                        }
                    }
                }).map(new MapFunction<Tuple3<String,ObjectNode,Integer>, String>() {
                    @Override
                    public String map(Tuple3<String, ObjectNode, Integer> noDupeInput) throws Exception {
                        return noDupeInput.f1.toString();
                    }
                }).split(new OutputSelector<String>() {
                    @Override
                    public Iterable<String> select(String s) {
                        List<String> output = new ArrayList<>();
                        output.add("regular");
                        output.add("avg");
                        return output;
                    }
                });

        DataStream<String> regular = stream.select("regular");

        // part 2
        // extract and return stream with unique key of sellerID+ItemID+price and count of 1
        // map to new key of type Line
        regular.map(new MapFunction<String, Tuple3<String,ObjectNode,Integer>>() {
            @Override
            public Tuple3<String,ObjectNode,Integer> map(String noDupeInput) throws Exception {
                ObjectNode on = parseJsonMutable(noDupeInput);
                System.out.println("bb" );
                //on.get("name").asText() +
                String key = on.get("typeLine").asText();
                return Tuple3.of(key, on, 1);
            }
        }).keyBy(0).window(TumblingEventTimeWindows.of(Time.hours(6)))
                .trigger(new Trigger<Tuple3<String, ObjectNode, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple3<String, ObjectNode, Integer> stringObjectNodeIntegerTuple3, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        if(l >= timeWindow.maxTimestamp()){
                            return TriggerResult.FIRE_AND_PURGE;
                        }else{
                            return TriggerResult.CONTINUE;
                        }
                    }
                })
                // item count using fold
                .fold(Tuple3.of("0", new ObjectNode(null), 0), new FoldFunction<Tuple3<String, ObjectNode, Integer>, Tuple3<String, ObjectNode, Integer>>() {
                    @Override
                    public Tuple3<String, ObjectNode, Integer> fold(Tuple3<String, ObjectNode, Integer> old, Tuple3<String, ObjectNode, Integer> current) throws Exception {
                        return Tuple3.of(current.f0,current.f1,current.f2+old.f2);
                    }
                    // revert back to single string stream
                }).map(new MapFunction<Tuple3<String,ObjectNode,Integer>, String>() {
            @Override
            public String map(Tuple3<String, ObjectNode, Integer> filteredInput) throws Exception {
                ObjectNode on = filteredInput.f1;
                String cleanName = makeNamePretty(on);
                on.put("cleanName", cleanName);
                on.put("count", filteredInput.f2);
                on.put("price", parseValue(on.get("note").asText()));
                on.put("privateMessage", createPM(on));
                return on.toString();
//                return "______";
            }
        }).addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));


        //part 3 compute windowed average in last 6 hours
        //parameters are (key) name, objectnode, count, price, price^2
        DataStream<String> avg = stream.select("avg");

        avg.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JsonNode jn = parseJson(s);
                //System.out.println("got here: " + jn.get("note"));
                try {
                    //System.out.println("got here: " + jn.get("price"));
                    return parseValue(jn.get("note").asText()) >=0;
                }catch(NullPointerException npe){
                    return false;
                }
            }
        })
                // map to key, rawdata, count, price, price^2
                .map(new MapFunction<String, Tuple5<String,ObjectNode,Integer,Double, Double>>() {
                    @Override
                    public Tuple5<String, ObjectNode, Integer, Double, Double> map(String raw) throws Exception {
                        ObjectNode on = parseJsonMutable(raw);
                        on.put("price", parseValue(on.get("note").asText()));
                        on.put("cleanName", makeNamePretty(on));
                        return Tuple5.of(makeNamePretty(on), on,
                                1, on.get("price").asDouble(), on.get("price").asDouble()*on.get("price").asDouble());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(6)))
                .trigger(new Trigger<Tuple5<String, ObjectNode, Integer, Double, Double>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple5<String, ObjectNode, Integer, Double, Double> stringObjectNodeIntegerDoubleDoubleTuple5, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        if(l >= timeWindow.maxTimestamp()){
                            return TriggerResult.FIRE_AND_PURGE;
                        }else{
                            return TriggerResult.CONTINUE;
                        }

                    }
                })
                // sum up price and price^2
                .fold(Tuple5.of("0", new ObjectNode(null), 0, 0D, 0D),
                        new FoldFunction<Tuple5<String, ObjectNode, Integer, Double, Double>,
                                Tuple5<String, ObjectNode, Integer, Double, Double>>() {
                            @Override
                            public Tuple5<String, ObjectNode, Integer, Double, Double>
                            fold(Tuple5<String, ObjectNode, Integer, Double, Double> old,
                                 Tuple5<String, ObjectNode, Integer, Double, Double> current) throws Exception {
                                return Tuple5.of(current.f0,current.f1,current.f2+old.f2,
                                        old.f3+current.f3, old.f4+current.f4);
                            }
                        })
                // return simple output of name+typeLine, mean, STD
                .map(new MapFunction<Tuple5<String,ObjectNode,Integer,Double,Double>, String>() {
                    @Override
                    public String map(Tuple5<String, ObjectNode, Integer, Double,Double> finalOutput) throws Exception {

//                String finalString = "\"name\": " + "\""+finalOutput.f0+ "\""
//                        + ", \"avgPrice\": "+ Double.toString(finalOutput.f3/finalOutput.f2)
//                        + ", \"STD\": " + Double.toString(getSTD(finalOutput.f2, finalOutput.f3, finalOutput.f4))
//                        + ", \"threshold\": " + Double.toString(finalOutput.f3/finalOutput.f2
//                        - 2*(getSTD(finalOutput.f2, finalOutput.f3, finalOutput.f4)));
                        Double avgPrice = finalOutput.f3/finalOutput.f2;
                        Double STD = getSTD(finalOutput.f2, finalOutput.f3, finalOutput.f4);
                        Double threshold = finalOutput.f3/finalOutput.f2
                                - 2*(getSTD(finalOutput.f2, finalOutput.f3, finalOutput.f4));
                        finalOutput.f1.put("avgPrice", avgPrice);
                        finalOutput.f1.put("STD", STD);
                        finalOutput.f1.put("threshold", threshold);
                        //ObjectNode finalJN = parseJsonMutable(finalString);
                        return finalOutput.f1.toString();
                    }
                }).addSink(new FlinkKafkaProducer09<String>(meanOut, new SimpleStringSchema(), outProps));

        env.execute();
    }


    // generate timestamp and watermark to allow for windowed stream
    private static class MyTimeStamp implements AssignerWithPeriodicWatermarks {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }
        @Override
        public long extractTimestamp(Object o, long l) {
            return System.currentTimeMillis();
        }
    }


    // turn raw json string into json object
    // for editing json operations
    private static ObjectNode parseJsonMutable(String raw) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = null;
        if(null != raw){
            rootNode = mapper.readTree(raw);
        }
        return (ObjectNode) rootNode;
    }

    // for read only json operations
    private static JsonNode parseJson(String raw) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = null;
        if(null != raw){
            rootNode = mapper.readTree(raw);
        }
        return rootNode;
    }

    // ayyyyyy lmao
    private static double getSTD(Integer count, Double sum, Double ss){
        return Math.sqrt(ss/count- (sum*sum)/count/count);

    }

    // remove tags from item name
    private static String makeNamePretty(ObjectNode on){
        String rawName = on.get("name").asText()+" "+on.get("typeLine").asText();
        String[] splitName = rawName.split("<<set:.+>><<set:.+>><<set:.+>>(.*?)");
        if(splitName.length >1){
            return splitName[1];
        }else{
            return rawName;
        }
    }


    // generate a private message to send to player to request purchase of item
    private static String createPM(ObjectNode on){
        String target = "@" + on.get("accountName").asText();
        String theName = on.get("cleanName").asText();
        String xpos = on.get("x").asText();
        String ypos = on.get("y").asText();
        String note = on.get("note").asText();
        String stashName = on.get("stashName").asText();
        String league = on.get("league").asText();
        return target +" Hi, I would like to buy your "
                +theName +" listed for "
                +note+" in " + league+ " (stash tab "
                +stashName+"; position: "
                +"left "+ xpos+", "
                +"top "+ypos+")";

        //" Hi, I would like to buy your Reach of the Council Spine Bow listed for 120 chaos in Breach (stash tab \"трейд\"; position: left 11, top 8)"
    }

    // convert the note to a numeric price value relative to chaos orbs
    // items that cannot be converted are given negative value
    private static double parseValue(String note){
        String dankNote = note.toLowerCase();
        // check if buyout
        try {
            if(dankNote.contains("~b/o".toLowerCase())){
                Pattern pattern = Pattern.compile("[0-9]+.*,*[0-9]*\\s");
                Matcher matcher = pattern.matcher(note);
                Double price = 0D;
                try {
                    matcher.find();
                    price = Double.parseDouble(matcher.group(0));
                } catch (Exception noRegEx){
                    price = -3D;
                }

                if (dankNote.contains("chaos")){
                    return price;
                }
                else if (dankNote.contains("exa")){
                    return price*83;
                }
                else if (dankNote.contains("jeweller")){
                    return price/12;
                }
                else if (dankNote.contains("fusing")){
                    return price/2.9;
                }
                else if (dankNote.contains("alchemy")){
                    return price/2.5;
                }
                else {
                    return -1;
                }
            }
            // problem formatting number skip and return -3
        }catch (NumberFormatException nfe){
            return -3;
        }

        return -2;
    }

}



//private static class FoldInWindow implements FoldFunction<Tuple5<String, Long, Integer, Double, Double>,
//        Tuple5<String, Long, Integer, Double, Double>> {
//    @Override
//    public Tuple5<String, Long, Integer, Double, Double>
//    fold(Tuple5<String, Long, Integer, Double, Double> old,
//         Tuple5<String, Long, Integer, Double, Double> current) throws Exception {
//        return Tuple5.of(current.f0,current.f1,current.f2+old.f2,
//                old.f3+current.f3, old.f4+current.f4);
//    }
//}
//
//private static class AggInWindow
//        implements WindowFunction<Tuple5<String, Long, Integer, Double, Double>,
//        Tuple5<String, Long, Integer, Double, Double>,String, TimeWindow> {
//
//    @Override
//    public void apply(String key,
//                      TimeWindow window,
//                      Iterable<Tuple5<String, Long, Integer, Double, Double>> aggs,
//                      Collector<Tuple5<String, Long, Integer, Double, Double>> collector) throws Exception {
//        Integer count = aggs.iterator().next().getField(3);
//        Double sums = aggs.iterator().next().getField(4);
//        Double ss = aggs.iterator().next().getField(5);
//        collector.collect(new Tuple5<String, Long, Integer, Double, Double>(key, window.getEnd(), count, sums, ss));
//    }
//}
