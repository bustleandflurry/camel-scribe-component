/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bustleandflurry.camel.component.scribe;

import java.net.InetAddress;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a scribe endpoint.
 */
public class ScribeEndpoint extends DefaultEndpoint {
	private static final transient Logger LOG = LoggerFactory.getLogger(ScribeComponent.class);
	
	public final static String SCRIBE_CATEGORY_HEADER = "scribe_category";
	
	private InetAddress address;
	private int port;
	private String category;
	private boolean splitEntries;
	
    public ScribeEndpoint() {
    }

    public ScribeEndpoint(String uri, ScribeComponent component) {
        super(uri, component);
        LOG.info("Endpoint URI: " + uri);
    }

    public ScribeEndpoint(String endpointUri) {
        super(endpointUri);
    }

    public Producer createProducer() throws Exception {
        return new ScribeProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new ScribeConsumer(this, processor);
    }

    public boolean isSingleton() {
        return true;
    }

	public InetAddress getAddress() {
		return address;
	}

	public void setAddress(InetAddress address) {
		this.address = address;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public boolean isSplitEntries() {
		return splitEntries;
	}

	public void setSplitEntries(boolean splitEntries) {
		this.splitEntries = splitEntries;
	}
    
	
    
}
