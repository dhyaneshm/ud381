package udacity.storm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import udacity.storm.tools.Rankable;
import udacity.storm.tools.Rankings;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt that counts the words that it receives
 */
public class TopNTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;
  
  //private static Logger logger = Logger.getLogger(TopNTweetBolt.class) ;
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TopNTweetBolt.class);

  // Map to store the count of the words
  private Map<String,Long> topTweets;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the map
    topTweets = new LinkedHashMap<String,Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
	  logger.info("Temp#-# test :" +tuple.getValue(0) +"--"+tuple.getSourceComponent());
    if(tuple.getSourceComponent().equals("total-ranking-bolt")){
		Map<String,Long> updatedTopTweets = new LinkedHashMap<String, Long>();
    	Rankings rankableList = (Rankings) tuple.getValue(0);
	    for (Rankable r: rankableList.getRankings()){
			  String word = r.getObject().toString();
			  Long count = r.getCount();
			  updatedTopTweets.put(word, count);
		  //redis.publish("WordCountTopology", word + "|" + Long.toString(count));
	    }
	    topTweets = updatedTopTweets ;
    }else if(tuple.getSourceComponent().equals("tweet-spout") ){
    	String currentTweet = (String) tuple.getValue(0);
    	logger.info("Temp#-# current Tweet :" +currentTweet);
    	if(StringUtils.isNotBlank(currentTweet)){
    		Map<String,Long> updatedTopTweets = new LinkedHashMap<String, Long>();
        	updatedTopTweets.putAll(topTweets) ;
        	logger.info("Temp#-# updatedTopTweets:" +updatedTopTweets.keySet());
        	for( Entry<String, Long> currrentEntry :updatedTopTweets.entrySet()){
        		if(currentTweet.contains(currrentEntry.getKey())){
        			collector.emit(new Values(currentTweet, currrentEntry.getValue()));
        		}
        	}
    	}
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'word' and 'count'

    // declare the first column 'word', second column 'count'
    outputFieldsDeclarer.declare(new Fields("tweets","count"));
  }
}
