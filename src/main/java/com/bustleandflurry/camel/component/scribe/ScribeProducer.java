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

import java.util.ArrayList;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bustleandflurry.camel.component.scribe.thrift.LogEntry;
import com.bustleandflurry.camel.component.scribe.thrift.ResultCode;
import com.bustleandflurry.camel.component.scribe.thrift.Scribe;

/**
 * The scribe producer.
 */
public class ScribeProducer extends DefaultProducer {
    private static final transient Logger LOG = LoggerFactory.getLogger(ScribeProducer.class);
    private ScribeEndpoint endpoint;
    
    private TFramedTransport transport;
    private Scribe.Client client;
    
    
    public ScribeProducer(ScribeEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Binding to server address: " + this.endpoint.getAddress() +" using port: " + String.valueOf(this.endpoint.getPort()));
        }
        
        transport = new TFramedTransport(new TSocket(this.endpoint.getAddress().getHostAddress(), this.endpoint.getPort()));
        TBinaryProtocol tBinaryProtocol = new  TBinaryProtocol(transport, false, false);
        client = new Scribe.Client(tBinaryProtocol);
    }
    
    public void process(Exchange exchange) throws Exception {
        System.out.println(exchange.getIn().getBody());    
        
        if (LOG.isDebugEnabled()) 
        	LOG.debug("Producing message: " + exchange.getIn().getBody());
        
        if (!transport.isOpen())
        	transport.open();
        
        LogEntry logEntry = new LogEntry(this.endpoint.getCategory(), exchange.getIn().getBody().toString());
        
        ArrayList<LogEntry> messages = new ArrayList<LogEntry>();
        messages.add(logEntry);
        
        ResultCode resultCode = client.Log(messages);
        
        if (resultCode.getValue() == 1)
        	throw new Exception("Scribe server returned: TRY_LATER");
        
        if (LOG.isDebugEnabled()) 
        	LOG.debug("Scribe server returned: " + resultCode.toString());
        
    }
    
    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (LOG.isDebugEnabled()) 
            LOG.debug("Starting scribe connection to address: " + this.endpoint.getAddress());
        transport.open();
    }

    @Override
    protected void doStop() throws Exception {
    	if (LOG.isDebugEnabled()) 
            LOG.debug("Stopping scribe connection to address: " + this.endpoint.getAddress());
        transport.close();
        super.doStop();
    }

}
