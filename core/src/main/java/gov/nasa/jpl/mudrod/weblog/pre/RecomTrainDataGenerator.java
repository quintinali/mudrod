package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.weblog.structure.RankingTrainData;
import gov.nasa.jpl.mudrod.weblog.structure.RecomTrainData;
import gov.nasa.jpl.mudrod.weblog.structure.Session;
import gov.nasa.jpl.mudrod.weblog.structure.SessionExtractor;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RecomTrainDataGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RecomTrainDataGenerator.class);

  public RecomTrainDataGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info("Starting generate recommendation train data.");
    startTime = System.currentTimeMillis();

    //String recomTrainFile = "E:\\data\\mudrod\\traing.txt";
    String recomTrainFile = props.getProperty("recommendation_dl_train_data");
    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<RecomTrainData> rankingTrainDataRDD = extractor.extractRecomTrainData(this.props, this.es, this.spark);

      JavaRDD<String> rankingTrainData_JsonRDD = rankingTrainDataRDD.map(f -> f.toJson());

      JavaRDD<String> tmpRDD = rankingTrainData_JsonRDD.coalesce(1);
      
      System.out.print(tmpRDD.count());
      tmpRDD.coalesce(1,true).saveAsTextFile(recomTrainFile);
      
      
      /*List<String> test = rankingTrainData_JsonRDD.collect();
      for(int i=0; i<test.size(); i++){
      	System.out.println(test.get(i));
      }*/

    } catch (Exception e) {
      e.printStackTrace();
    }
    
    //medium test: test one mounth
    /*List<RecomTrainData> alldatas = new ArrayList<RecomTrainData>();
    SessionExtractor extractor = new SessionExtractor();
    List<String> sessionIdList = extractor.getSessions(props, es, "podaaclog201612.w5.gz");
    System.out.println(sessionIdList.toString());
    Session session = new Session(props, es);
    for(int i=0; i<sessionIdList.size();i++){
    	String sessionId = sessionIdList.get(i).substring(0, sessionIdList.get(i).indexOf(","));
    	List<RecomTrainData> datas = session.getRecomTrainData("podaaclog201612.w3.gz", "cleanupLog", sessionId);
    	alldatas.addAll(datas);
    }
    for(int i=0; i<alldatas.size(); i++){
    	System.out.println(alldatas.get(i).toJson());
    }*/
    
   
    //small test: test one session
    /*SessionExtractor extractor = new SessionExtractor();
    Session session = new Session(props, es);
    List<RecomTrainData> datas = session.getRecomTrainData("podaaclog201612.w5.gz", "cleanupLog", "68.64.169.242@1");
    for(int i=0; i<datas.size(); i++){
    	System.out.println(datas.get(i).toJson());
    }*/

    endTime = System.currentTimeMillis();
    LOG.info("recommendation train data generation complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }
}
