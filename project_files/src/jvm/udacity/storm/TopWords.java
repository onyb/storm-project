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
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

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
    }

    public void execute(Tuple tuple){
        String tweet = tuple.getStringByField("original-tweet");
        String county = (String) tuple.getStringByField("county_id");
        int sentiment = tuple.getIntegerByField("sentiment");
        collector.emit(new Values(tweet, county, sentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet", "county_id",
                                                "sentiment"));
    }
}
