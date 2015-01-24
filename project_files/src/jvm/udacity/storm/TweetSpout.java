package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.URLEntity;
import udacity.storm.tools.SentimentAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;
import java.util.Arrays;


/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */

public class TweetSpout extends BaseRichSpout
{
    // Twitter API authentication credentials
    String custkey, custsecret;
    String accesstoken, accesssecret;

    // To output tuples from spout to Parse Tweet Bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<String> queue = null;
  
    private List<String> keywords = Arrays.asList(
//Domestic Security
"assassination", "attack", "domestic security", "drill", "exercise", "cops",
"law enforcement", "authorities", "disaster assistance", "disaster management",
"dndo", "domestic nuclear detection office", "national preparedness",
"mitigation", "prevention","response", "recovery", "dirty bomb",
"domestic nuclear detection", "emergency detection", "emergency response",
"first responder", "homeland security", "maritime domain awareness", "mda",
"militia", "initiative", "shooting", "shots fired", "evacuation", "deaths",
"hostage", "explosion", "explosive", "police", "dmat", "organized crime",
"gangs", "national security", "state of emergency", "security", "breach",
"threat", "standoff", "swat", "screening", "lockdown", "bomb squad",
"bomb threat", "crash", "looting", "riot", "emergency landing", "pipe bomb",
"incident", "facility",

//HAZMAT and Nuclear
"hazmat", "nuclear", "chemical spill", "suspicious package",
"suspicious device", "toxic", "national laboratory", "nuclear facility",
"nuclear threat", "cloud", "plume", "radiation", "radioactive", "leak",
"biological infection", "biological event", "chemical", "chemical burn",
"biological", "epidemic", "hazardous", "hazardous material incident",
"industrial spill", "infection", "powder", "white powder", "gas",
"spillover", "antrax", "blister agent", "chemical agent", "exposure", "burn",
"nerve agent", "ricin", "sarin", "north korea",

//Health Concern + H1N1
"outbreak", "contamination", "exposure", "virus", "evacuation", "bacteria",
"recall", "ebola", "food poisoning", "food and mouth", "fmd", "h5n1", "avian",
"flu", "strain", "quarantine", "h1n1", "vaccine", "salmonella", "small pox",
"plague", "human to human", "human to animal", "influenza",
"center for disease control", "CDC", "drug administration", "FDA",
"public health", "toxic", "agro terror", "tuberculosis", "TB", "tamiflu",
"norvo virus", "epidemic", "agriculture", "listeria", "symptoms", "mutation",
"resistant", "antiviral", "wave", "pandemic", "water borne", "air borne",
"sick", "swine", "pork", "world health organization", "who",
"viral hemorrhagic fever", "e. coli",

//Infrastructure Security
"infrastructure security", "airport", "cikr", "amtrak", "collapse",
"computer infrastructure", "communications", "infrastructure",
"telecommunications", "critical infrastructure", "national infrastructure",
"metro", "wmata", "airplane", "chemical fire", "subway", "bart", "marta",
"port authority", "nbic", "transportation security", "grid", "power", "smart",
"body scanner", "electric", "failure", "outage", "black out", "brown out",
"port", "dock", "bridge", "cancelled", "delays", "service disruption",
"power lines",

//South West Border Violence
"drug cartel", "violence", "gang", "drug", "narcotics", "cocaine", "marijuana",
"heroin", "border", "mexico", "cartel", "southwest", "juarez", "sinaloa",
"tijuana", "torreon", "yuma", "tucson", "decapitated", "us consulate",
"consular", "el paso", "fort hancock", "san diego", "ciudad juarez", "nogales",
"sonora", "colombia", "mara salvatrucha", "ms13", "ms-13", "drug war",
"mexican army", "methamphetamine", "cartel de golfo", "gulf cartel",
"la familia", "reynosa", "nuevo leon", "narcos", "narco banners", "los zetas",
"shootout", "execution", "gunfight", "trafficking", "kidnap", "calderon",
"reyosa", "bust", "tamaulipas", "meth lab", "drug trade", "illegal immigrants",
"smuggling", "smugglers", "matamoros", "michoacana", "guzman",
"arellano-felix", "beltran-leyva", "barrio azteca", "mexicles",
"artistic assasins", "new federation",

//Terrorism
"terrorism", "al qaeda", "terror", "attack", "iraq", "afghanistan", "iran",
"pakistan", "agro", "environmental terrorist", "eco terrorist",
"conventional weapon", "target", "weapons grade", "enriched", "nuclear",
"chemical weapon", "biological weapon", "ammonium nitrate", "ied",
"improvised explosive device", "abu sayyaf", "farc", "hamas", "ira", "eta",
"irish republican army", "euskadi ta askatasuna", "basque separatists",
"hezbollah", "tamil tigers", "plf", "palestine liberation front", "plo",
"palestine liberation organization", "car bomb", "jihad", "taliban",
"weapons cache", "suicide bomber", "suicide attack", "suspicious substance",
"aqap", "al qaeda arabian peninsula", "aqim", "al qaeda in islamic maghreb",
"yemen", "pirates", "extremism", "somalia", "nigeria", "radical", "al-shabaab",
"home grown", "plot", "nationalist", "recruitment", "fundamentalism", "islam",

//Weather/Disaster/Emergency
"emergency", "hurricane", "tornado", "twister", "tsunami", "earthquake",
"tremor", "flood", "storm", "crest", "temblor", "extreme weather",
"forest fire", "brush fire", "ice", "stranded", "stuck", "help", "hail",
"wildfire", "magnitude", "avalanche", "typhoon", "shelter", "disaster",
"snow", "blizzard", "sleet", "mud slide", "erosion", "power outage", "warning",
"watch", "lightening", "aid", "relief", "closure", "interstate", "burst",

//Cyber Security
"cyber security", "botnet", "ddos", "denial of service", "malware", "virus",
"trojan", "keylogger", "cyber command", "2600", "spammer", "phishing",
"rootkit", "phreaking", "cain and abel", "brute force", "sql injection",
"cyber attack", "cyber terror", "hacker", "china", "conficker", "worm",
"scammer", "social media");

    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener
    {
        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status)
        {
            // add the tweet into the queue buffer
            String geoInfo = "37.7833,122.4167";
            String urlInfo = "n/a";
            if(status.getGeoLocation() != null){
                geoInfo = String.valueOf(status.getGeoLocation().getLatitude())
                          + "," + String.valueOf(status.getGeoLocation()
                                                       .getLongitude());

                if(status.getURLEntities().length > 0){
                    for(URLEntity urlE: status.getURLEntities()){
                        urlInfo = urlE.getURL();
                    }
                }
                queue.offer(status.getText() + "DELIMITER" + geoInfo + "DELIMITER" + urlInfo);
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn){}

        @Override
        public void onTrackLimitationNotice(int i){}

        @Override
        public void onScrubGeo(long l, long l1){}

        @Override
        public void onStallWarning(StallWarning warning){}

        @Override
        public void onException(Exception e)
        {
            e.printStackTrace();
        }
    };

  /**
   * Constructor for tweet spout that accepts the credentials
   */

    public TweetSpout(
        String                key,
        String                secret,
        String                token,
        String                tokensecret)
    {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
    }

    @Override
    public void open(
        Map                     map,
        TopologyContext         topologyContext,
        SpoutOutputCollector    spoutOutputCollector)
    {
        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<String>(1000);
        SentimentAnalyzer.init();
        // save the output collector for emitting tuples
        collector = spoutOutputCollector;

        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
            new ConfigurationBuilder()
                .setOAuthConsumerKey(custkey)
                .setOAuthConsumerSecret(custsecret)
                .setOAuthAccessToken(accesstoken)
                .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.locations(new double[][]{
                    new double[]{-124.848974,24.396308},
                    new double[]{-66.885444,49.384358}
                    });
        tweetFilterQuery.language(new String[]{"en"});

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());
        twitterStream.filter(tweetFilterQuery);

        // start the sampling of tweets
        twitterStream.sample();
    }

    @Override
    public void nextTuple()
    {
        // try to pick a tweet from the buffer
        String ret = queue.poll();
        String geoInfo;
        String originalTweet;

        // if no tweet is available, wait for 50 ms and return
        if (ret==null){
            Utils.sleep(50);
            return;
        }
        else{
            geoInfo = ret.split("DELIMITER")[1];
            originalTweet = ret.split("DELIMITER")[0];
        }

        if(geoInfo != null && !geoInfo.equals("n/a")){
            for(String key: keywords){
                if(originalTweet.toLowerCase().contains(key)){
                    System.out.println("\n|| WORD FLAGGED: " + key + " ||");
                    int sentiment = SentimentAnalyzer.findSentiment(originalTweet)-2;
                    System.out.println("|| ORIGINAL TWEET: " + originalTweet + " ||");
                    System.out.println("|| TWEET SENTIMENT SCORE: " + String.valueOf(sentiment) + " ||\n");
                    collector.emit(new Values(ret, sentiment));
                }
            }
        }
    }

    @Override
    public void close()
    {
        // shutdown the stream when we are going to exit
        twitterStream.shutdown();
    }

  /**
   * Component specific configuration
   */

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        // create the component config
        Config ret = new Config();

        // set the parallelism for this spout to be 1
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        /* Tell Storm the schema of the output tuple for this spout
           tuple consists of a single column called 'tweet' */
        outputFieldsDeclarer.declare(new Fields("tweet", "sentiment"));
    }
}
