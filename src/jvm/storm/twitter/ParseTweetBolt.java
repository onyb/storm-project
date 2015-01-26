package storm.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import storm.twitter.tools.CountiesLookup;

/**
 * A bolt that parses the tweet into words
 */

public class ParseTweetBolt extends BaseRichBolt
{
    OutputCollector collector;
    CountiesLookup clookup;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {
        collector = outputCollector;
        clookup= new CountiesLookup();
    }

    @Override
    public void execute(Tuple tuple)
    {
        String originalTweet = tuple.getStringByField("tweet").split("DELIMITER")[0];

        double latitude = Double.parseDouble(tuple.getStringByField("tweet")
                                                  .split("DELIMITER")[1]
                                                  .split(",")[0]);

        double longitude = Double.parseDouble(tuple.getStringByField("tweet")
                                                   .split("DELIMITER")[1]
                                                   .split(",")[1]);

        String county_id = clookup.getCountyCodeByGeo(latitude, longitude);

        int sentiment = tuple.getIntegerByField("sentiment");

        collector.emit(new Values(originalTweet, county_id, sentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        /* Tell Storm the schema of the output tuple for this spout
           tuple consists of a single column called 'tweet' */
        declarer.declare(new Fields("tweet", "county_id", "sentiment"));
    }
}
