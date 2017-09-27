/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Supports ability to remove raw logs after processing is finished
 */
public class RemoveRawLog extends LogAbstract {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RemoveRawLog.class);

	public RemoveRawLog(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
	}

	@Override
	public Object execute() {
		LOG.info("Starting raw log removal.");
		startTime = System.currentTimeMillis();
		//es.deleteAllByQuery(logIndex, httpType, QueryBuilders.matchAllQuery());
		//es.deleteAllByQuery(logIndex, ftpType, QueryBuilders.matchAllQuery());
		try {
			deleteType(logIndex, httpType);
			deleteType(logIndex, ftpType);
		} catch (InterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		endTime = System.currentTimeMillis();
		es.refreshIndex();
		LOG.info("Raw log removal complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
		return null;
	}

	@Override
	public Object execute(Object o) {
		return null;
	}

	public void deleteType(String index, String type) throws InterruptedException, IOException {
		String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE);
		if (processingType.equals("sequential")) {
			deleteTypeInSequential(index, type);
		} else if (processingType.equals("parallel")) {
			deleteTypeInParallel(index, type);
		}
	}

	/**
	 * Check crawler by request sending rate, which is read from configruation
	 * file
	 *
	 * @throws InterruptedException
	 *             InterruptedException
	 * @throws IOException
	 *             IOException
	 */
	public void deleteTypeInSequential(String index, String type) throws InterruptedException, IOException {
		es.deleteAllByQuery(index, type, QueryBuilders.matchAllQuery());
		LOG.info("delete type {}", type);
	}

	void deleteTypeInParallel(String index, String type) throws InterruptedException, IOException {

		JavaRDD<String> userRDD = getUserRDD(type);
		LOG.info("Original User count: {}", userRDD.count());

		int userCount = 0;
		userCount = userRDD.mapPartitions((FlatMapFunction<Iterator<String>, Integer>) iterator -> {
			ESDriver tmpES = new ESDriver(props);
			//tmpES.createBulkProcessor();
			List<Integer> realUserNums = new ArrayList<>();
			while (iterator.hasNext()) {
				String user = iterator.next();
				tmpES.deleteAllByQuery(index, type, QueryBuilders.termQuery("IP", user));
				realUserNums.add(1);
			}
			//tmpES.destroyBulkProcessor();
			tmpES.close();
			return realUserNums.iterator();
		}).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

		LOG.info("delete type {}", type);
	}

}
