package com.storm.tutorial.sg;
//import storm configuration packages
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {
   public static void main(String[] args) throws Exception {
	   
      //Create Config instance for cluster configuration
      Config config = new Config();
      config.setDebug(false);
      config.setMessageTimeoutSecs(1);
		
      //
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

      builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
         .shuffleGrouping("call-log-reader-spout");

      builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
         .fieldsGrouping("call-log-creator-bolt", new Fields("call"));
			
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
      // Thread.sleep(60000);
		
      //Stop the topology
      // System.out.println("Killing topology");
      // cluster.killTopology("LogAnalyserStorm");
      
      // System.out.println("Shutting down cluster");
      // cluster.shutdown();
   }
}