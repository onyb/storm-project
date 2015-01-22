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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import udacity.storm.tools.CountiesLookup;

/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;
  StringBuilder result;
  private String[] skipWords = {"rt", "to", "me","la","on","that","que",
    "followers","watch","know","not","have","like","I'm","new","good","do",
    "more","es","te","followers","Followers","las","you","and","de","my","is",
    "en","una","in","for","this","go","en","all","no","don't","up","are",
    "http","http:","https","https:","http://","https://","with","just","your",
    "para","want","your","you're","really","video","it's","when","they","their","much",
    "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
    "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
    "make","take","This","from","about","como","esta","follows","followed"};

  //MaxentTagger tagger ;
  CountiesLookup clookup ;
  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    //tagger	= new MaxentTagger("models/english-caseless-left3words-distsim.tagger");
    clookup= new CountiesLookup();

  	 result = new StringBuilder();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the 1st column 'tweet' from tuple
    String tweet = tuple.getStringByField("tweet").split("DELIMITER")[0];
    double latitude = Double.parseDouble(tuple.getStringByField("tweet").split("DELIMITER")[1].split(",")[0]);
    double longitude = Double.parseDouble(tuple.getStringByField("tweet").split("DELIMITER")[1].split(",")[1]);
    String county_id = clookup.getCountyCodeByGeo(latitude, longitude);

    int sentiment = tuple.getIntegerByField("sentiment");
    
    String url = tuple.getString(0).split("DELIMITER")[2];

    
    // provide the delimiters for splitting the tweet
    String delims = "[ .,?!]+";

    String [] posTweet = {"", "", ""};
    //String [] posTweet = new String[]{"i" ,"got" ,"ebola"};
    if(posTweet != null)
    {    
    	String[] tokens = tweet.split(delims);
      	System.out.print("\tParseTweetBolt\tDEBUG:" + posTweet + ", URL: " + url + "\n");
	    // for each token/word, emit it
      	result.setLength(0);
      	int n = tokens.length;
	    for (int i = 0; i < n; i++) {
	    	if(!Arrays.asList(skipWords).contains(tokens[i])){
	    		result.append(tokens[i]); 
	   	      }
	    }
	    
	    collector.emit(new Values(tweet, result.toString(), posTweet[0], posTweet[1], posTweet[2], county_id, url, sentiment));

		
    }
  }  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("original-tweet", "tweet-word", "noun", "verb", "object", "county_id", "url", "sentiment"));
  }

}
