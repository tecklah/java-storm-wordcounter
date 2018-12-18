package com.storm.sg;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CounterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(CounterBolt.class);
	
	private HashMap<String, Integer> wordCounter = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String word = input.getString(0);
		if (wordCounter.containsKey(word)) {
			wordCounter.put(word, wordCounter.get(word).intValue() + 1);
		} else {
			wordCounter.put(word, 1);
		}
		
		System.out.println(word + ":" + wordCounter.get(word));
		
		if (wordCounter.get(word).intValue() > 3)
		{
			collector.emit("alert-stream", new Values(word));
		}
		else
		{
			collector.emit("noalert-stream", new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("alert-stream", new Fields("word"));
		declarer.declareStream("noalert-stream", new Fields("word"));
	}
}