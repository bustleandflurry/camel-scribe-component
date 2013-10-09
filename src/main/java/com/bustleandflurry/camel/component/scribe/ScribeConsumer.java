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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bustleandflurry.camel.component.scribe.thrift.LogEntry;
import com.bustleandflurry.camel.component.scribe.thrift.ResultCode;
import com.bustleandflurry.camel.component.scribe.thrift.Scribe;
import com.facebook.fb303.fb_status;



/**
 * The Scribe consumer.
 */
public class ScribeConsumer extends DefaultConsumer {
	private static final transient Logger LOG = LoggerFactory.getLogger(ScribeConsumer.class);
    private final ScribeEndpoint endpoint;
    
    
    private Thread thread;

    public ScribeConsumer(ScribeEndpoint endpoint, Processor processor) throws Exception {
        super(endpoint, processor);
        this.endpoint = endpoint;
        
        if (LOG.isDebugEnabled()) 
            LOG.debug("Initializing scribe consumer");
        
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(this.endpoint.getAddress(), endpoint.getPort());
        
        ScribeHandler scribeHandler = new ScribeHandler(endpoint);
		final Scribe.Processor scribeProcessor = new Scribe.Processor(scribeHandler);
		
		Runnable scribeThread = new Runnable() {
			public void run() {
					try {
						scribeServer(scribeProcessor, inetSocketAddress);
					} catch (Exception e) {
						try {
							throw e;
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					}
			}
		}; 
        
		thread = new Thread(scribeThread);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (LOG.isDebugEnabled()) 
            LOG.debug("Binding to server address: " + this.endpoint.getAddress() +" using port: " + String.valueOf(this.endpoint.getPort()));

        thread.start();
        
    }

    @Override
    protected void doStop() throws Exception {
        if (LOG.isDebugEnabled()) 
            LOG.debug("Unbinding from server address: " + this.endpoint.getAddress().getHostAddress() +" using port: " + String.valueOf(this.endpoint.getPort()));
        
        thread.stop();
        super.doStop();
        
    }
    
    private static void scribeServer(Scribe.Processor scribeProcessor, InetSocketAddress inetSocketAddress ) throws Exception {
    	try {
			
	        TServerSocket tServerSocket = new TServerSocket(inetSocketAddress);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(tServerSocket).processor(scribeProcessor).protocolFactory(new TBinaryProtocol.Factory()).transportFactory(new TFramedTransport.Factory()));
			server.serve();
		} catch (TTransportException e) {
			throw e;
		}
    }
    
    
	public class ScribeHandler implements Scribe.Iface {
		private ScribeEndpoint endpoint;
		
		public ScribeHandler(ScribeEndpoint endpoint) {
    		if (LOG.isDebugEnabled()) 
    			LOG.debug("ScribeHandler Initialized");
    		this.endpoint = endpoint;
		}

    	@Override
    	public ResultCode Log(List<LogEntry> scribeMessages) throws TException {
    		
    		Exchange exchange = getEndpoint().createExchange();
			Message message = exchange.getIn();
    		
			ResultCode resultCode = ResultCode.findByValue(1); 
    		
    		if(this.endpoint.isSplitEntries()) {
    			ListIterator<LogEntry> litr = scribeMessages.listIterator();
	    		while(litr.hasNext()){
	    			LogEntry logEntry = litr.next();
	        		message.setBody(logEntry.message);
	        		message.setHeader(ScribeEndpoint.SCRIBE_CATEGORY_HEADER, logEntry.category);	
	        		exchange.setIn(message);
	        		
	        		try {
	    				getProcessor().process(exchange);
	    			} catch (Exception e) {
	    				getExceptionHandler().handleException(e);
	    				resultCode = ResultCode.findByValue(1); //TRY_LATER
	    			}
	        		resultCode = ResultCode.findByValue(0); //OK
	    		}
    		} else {
    			message.setBody(scribeMessages);
        		message.setHeader(ScribeEndpoint.SCRIBE_CATEGORY_HEADER, scribeMessages.get(0).category);
        		exchange.setIn(message);
        		
        		try {
    				getProcessor().process(exchange);
    			} catch (Exception e) {
    				getExceptionHandler().handleException(e);
    				resultCode = ResultCode.findByValue(1); //TRY_LATER
    			}
        		resultCode = ResultCode.findByValue(0); //OK
    		}
    		
    		return resultCode;
    	}

		@Override
		public long aliveSince() throws TException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getCounter(String arg0) throws TException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Map<String, Long> getCounters() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getCpuProfile(int arg0) throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getName() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getOption(String arg0) throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Map<String, String> getOptions() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public fb_status getStatus() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getStatusDetails() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getVersion() throws TException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void reinitialize() throws TException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setOption(String arg0, String arg1) throws TException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void shutdown() throws TException {
			// TODO Auto-generated method stub
			
		}

    
    }
    
    
}
