package com.storm.sg;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * @author chengsoonteck
 * This bolt is called by CounterBolt via "alert-stream" stream.
 * See how it is configured on KafkaStorm class.
 */
public class AlertBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(AlertBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String word = input.getString(0);
		System.out.println("The word " + word + " has exceed the threshold.");
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
