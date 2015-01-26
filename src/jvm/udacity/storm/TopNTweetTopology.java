package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

class TopNTweetTopology
{
    public static void main(String[] args) throws Exception
    {
        /* Create the Storm Topology using TopologyBuilder */
        TopologyBuilder builder = new TopologyBuilder();

        /*
         * In order to create the spout, you need to get twitter credentials
         * If you need to use Twitter firehose/Tweet stream for your idea,
         * create a set of credentials by following the instructions at
         *
         * https://dev.twitter.com/discussions/631
         *
         */

        TweetSpout tweetSpout = new TweetSpout(
            "", /* Consumer Key (API Key) */
            "", /* Consumer Secret (API Secret) */
            "", /* Access Token */
            ""  /* Access Token Secret */
        );

        /* Connect Tweet Spout to the topology with parallelism of 1 */
        builder.setSpout("tweet-spout", tweetSpout, 1);

        /* Connect Parse Tweet Bolt to Tweet Spout using Shuffle Grouping with parallelism of 10 */
        builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

        /* Connect Report Bolt to Parse Tweet Bolt using Global Grouping with a parallelism of 1 */
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("parse-tweet-bolt");

        /* Create the default Config object and set it in debugging mode */
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0){
            /*Run the topology in a live cluster */

            /* Set the number of workers for running all spout and bolt tasks */
            conf.setNumWorkers(3);

            /* Create the topology and submit with Config object */
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        }
        else{
            /* Run topology in a simulated local cluster */

            /* Set the number of threads to run (similar to the number of workers in live cluster) */
            conf.setMaxTaskParallelism(4);

            /* Create the local cluster instance */
            LocalCluster cluster = new LocalCluster();

            /* Submit the topology to the local cluster */
            cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

            /* Run topology for 300s. Note that topologies never terminate! */
            Utils.sleep(300000000);
            cluster.killTopology("tweet-word-count");

            /* Shut down the cluster */
            cluster.shutdown();
        }
    }
}
