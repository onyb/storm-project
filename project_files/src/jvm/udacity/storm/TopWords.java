package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import udacity.storm.spout.RandomSentenceSpout;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import udacity.storm.spout.RandomSentenceSpout;
import udacity.storm.tools.SentimentAnalyzer;


public class TopWords extends BaseRichBolt
{
    private OutputCollector collector;
    private Map<String, Integer> countNouns;
    private Map<String, Integer> countVerbs;
    private Map<String, Integer> countDO;
    private Map<String, Integer> SentimentDistribution;
    Integer val;
    double alpha;

    @Override
    public void prepare(
        Map              map,
        TopologyContext  topologyContext,
        OutputCollector  outputCollector)
    {
        collector = outputCollector;
        SentimentDistribution = new HashMap<String, Integer>();
        alpha = 0.2;
    }

    public double getAvg(String county)
    {
        double sum = 0;
        double total_count = 0.001;
        for(int i=-1; i< 2; i++){
            if(SentimentDistribution.containsKey(county + " "
                                                 + String.valueOf(i))){
                sum += SentimentDistribution.get(county + " "
                                                 + String.valueOf(i))
                       *(i+1.0)/3.0;
                total_count += SentimentDistribution.get(county + " "
                                                         + String.valueOf(i));
            }
        }
        return sum/total_count;
    }

    public void execute(Tuple tuple)
    {
        String tweet = tuple.getStringByField("original-tweet");
        String county = (String) tuple.getStringByField("county_id");
        String url = tuple.getStringByField("url");
        int sentiment = tuple.getIntegerByField("sentiment");
        System.out.println("\n\n\nBEFORE: " + sentiment + "\n\n\n");
        String sentimentKey = county + " " + String.valueOf(sentiment);
        double reportSentiment = 0.5;

        if(SentimentDistribution.get(sentimentKey) == null){
            SentimentDistribution.put(sentimentKey,0);
        }

        SentimentDistribution.put(sentimentKey,
                                  SentimentDistribution.get(sentimentKey) + 1);

        reportSentiment = Math.max(Math.min(1.0, getAvg(county)*3), 0.0);
        System.out.println("\n\n\nAFTER: " + reportSentiment + "\n\n\n");

        collector.emit(new Values(tweet, county, url, reportSentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet", "county_id", "url", "sentiment"));
    }
}
