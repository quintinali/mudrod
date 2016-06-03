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
package esiptestbed.mudrod.discoveryengine;

import java.io.Serializable;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.process.MetadataAnalyzer;

public class MetadataDiscoveryEngine extends DiscoveryEngineAbstract implements Serializable {	
	
	public MetadataDiscoveryEngine(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	public void preprocess() {
		// TODO Auto-generated method stub

	}

	public void process() {
		// TODO Auto-generated method stub
		print("*****************metadata processing starts******************", 3);

		DiscoveryStepAbstract svd = new MetadataAnalyzer(this.config, this.es, this.spark);
		svd.execute();
		
		endTime=System.currentTimeMillis();
		System.out.println("*****************metadata processing ends******************Took " + (endTime-startTime)/1000+"s");
	}

	public void output() {
		// TODO Auto-generated method stub

	}
}
