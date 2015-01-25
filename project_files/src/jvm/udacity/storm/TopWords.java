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
    private Map<String, Integer> SentimentDistribution;
    double alpha;

    @Override
    public void prepare(
        Map                 map,
        TopologyContext     topologyContext,
        OutputCollector     outputCollector){
        collector = outputCollector;
        SentimentDistribution = new HashMap<String, Integer>();
        alpha = 0.2;
    }

    public void execute(Tuple tuple){
        String tweet = tuple.getStringByField("original-tweet");
        String county = (String) tuple.getStringByField("county_id");
        String url = tuple.getStringByField("url");
        int sentiment = tuple.getIntegerByField("sentiment");
        String sentimentKey = county + " " + String.valueOf(sentiment);
        if (SentimentDistribution.get(sentimentKey) == null){
            SentimentDistribution.put(sentimentKey,0);
        }
        SentimentDistribution.put(sentimentKey, SentimentDistribution.get(sentimentKey) + 1);
        collector.emit(new Values(tweet, county, url, sentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet", "county_id", "url",
                                                "sentiment"));
    }
}
