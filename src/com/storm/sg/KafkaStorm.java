package com.storm.sg;

import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaStorm {
	
	/**
	 * Run this program to create and submit the topology to Storm.
	 */
	public static void main(String[] args) throws Exception {

		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		config.setMessageTimeoutSecs(1);

		// ZooKeeper connection string
		BrokerHosts hosts = new ZkHosts("127.0.0.1:2181");

		//Creating SpoutConfig Object
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "/tmp/kafka-logs", "1");
		System.out.println("Spout ID: " + spoutConfig.id);
		System.out.println("Zookeeper root: " + spoutConfig.zkRoot);
			
		//convert the ByteBuffer to String.
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.ignoreZkOffsets = false;
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
		//Assign SpoutConfig to KafkaSpout.
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		// Build topology to consume message from kafka and print them on console
		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		// Create KafkaSpout instance using Kafka configuration and add it to topology
		topologyBuilder.setSpout("kafka-spout", new KafkaSpout(spoutConfig), 1);
		
		// Route the output of Kafka Spout to word counter bolt
		topologyBuilder.setBolt("counter-bolt", new CounterBolt()).shuffleGrouping("kafka-spout");
		topologyBuilder.setBolt("alert-bolt", new AlertBolt()).shuffleGrouping("counter-bolt","alert-stream");
		topologyBuilder.setBolt("noalert-bolt", new NoAlertBolt()).shuffleGrouping("counter-bolt","noalert-stream");

		// Submit topology to local cluster i.e. embedded storm instance in eclipse
		final LocalCluster localCluster = new LocalCluster();
		
		localCluster.submitTopology("kafka-topology", new HashMap<>(), topologyBuilder.createTopology());

	}
}
