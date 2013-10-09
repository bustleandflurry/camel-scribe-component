/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.bustleandflurry.camel.component.scribe.thrift;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scribe {

  public interface Iface extends com.facebook.fb303.FacebookService.Iface {

    public ResultCode Log(List<LogEntry> messages) throws org.apache.thrift.TException;

  }

  public interface AsyncIface extends com.facebook.fb303.FacebookService .AsyncIface {

    public void Log(List<LogEntry> messages, org.apache.thrift.async.AsyncMethodCallback<AsyncClient.Log_call> resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends com.facebook.fb303.FacebookService.Client implements org.apache.thrift.TServiceClient, Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      this(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot)
    {
      super(iprot, oprot);
    }

    public ResultCode Log(List<LogEntry> messages) throws org.apache.thrift.TException
    {
      send_Log(messages);
      return recv_Log();
    }

    public void send_Log(List<LogEntry> messages) throws org.apache.thrift.TException
    {
      oprot_.writeMessageBegin(new org.apache.thrift.protocol.TMessage("Log", org.apache.thrift.protocol.TMessageType.CALL, ++seqid_));
      Log_args args = new Log_args();
      args.setMessages(messages);
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public ResultCode recv_Log() throws org.apache.thrift.TException
    {
      org.apache.thrift.protocol.TMessage msg = iprot_.readMessageBegin();
      if (msg.type == org.apache.thrift.protocol.TMessageType.EXCEPTION) {
        org.apache.thrift.TApplicationException x = org.apache.thrift.TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      if (msg.seqid != seqid_) {
        throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID, "Log failed: out of sequence response");
      }
      Log_result result = new Log_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "Log failed: unknown result");
    }

  }
  public static class AsyncClient extends com.facebook.fb303.FacebookService.AsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void Log(List<LogEntry> messages, org.apache.thrift.async.AsyncMethodCallback<Log_call> resultHandler) throws org.apache.thrift.TException {
      checkReady();
      Log_call method_call = new Log_call(messages, resultHandler, this, protocolFactory, transport);
      this.currentMethod = method_call;
      manager.call(method_call);
    }

    public static class Log_call extends org.apache.thrift.async.TAsyncMethodCall {
      private List<LogEntry> messages;
      public Log_call(List<LogEntry> messages, org.apache.thrift.async.AsyncMethodCallback<Log_call> resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.messages = messages;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("Log", org.apache.thrift.protocol.TMessageType.CALL, 0));
        Log_args args = new Log_args();
        args.setMessages(messages);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public ResultCode getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_Log();
      }
    }

  }

  public static class Processor extends com.facebook.fb303.FacebookService.Processor implements org.apache.thrift.TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(Iface iface)
    {
      super(iface);
      iface_ = iface;
      processMap_.put("Log", new Log());
    }

    private Iface iface_;

    public boolean process(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException
    {
      org.apache.thrift.protocol.TMessage msg = iprot.readMessageBegin();
      ProcessFunction fn = processMap_.get(msg.name);
      if (fn == null) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, org.apache.thrift.protocol.TType.STRUCT);
        iprot.readMessageEnd();
        org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
        oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(msg.name, org.apache.thrift.protocol.TMessageType.EXCEPTION, msg.seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return true;
      }
      fn.process(msg.seqid, iprot, oprot);
      return true;
    }

    private class Log implements ProcessFunction {
      public void process(int seqid, org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException
      {
        Log_args args = new Log_args();
        try {
          args.read(iprot);
        } catch (org.apache.thrift.protocol.TProtocolException e) {
          iprot.readMessageEnd();
          org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.PROTOCOL_ERROR, e.getMessage());
          oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("Log", org.apache.thrift.protocol.TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        iprot.readMessageEnd();
        Log_result result = new Log_result();
        result.success = iface_.Log(args.messages);
        oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("Log", org.apache.thrift.protocol.TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

  }

  public static class Log_args implements org.apache.thrift.TBase<Log_args, Log_args._Fields>, java.io.Serializable, Cloneable   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Log_args");

    private static final org.apache.thrift.protocol.TField MESSAGES_FIELD_DESC = new org.apache.thrift.protocol.TField("messages", org.apache.thrift.protocol.TType.LIST, (short)1);

    public List<LogEntry> messages;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      MESSAGES((short)1, "messages");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // MESSAGES
            return MESSAGES;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments

    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.MESSAGES, new org.apache.thrift.meta_data.FieldMetaData("messages", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
              new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LogEntry.class))));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Log_args.class, metaDataMap);
    }

    public Log_args() {
    }

    public Log_args(
      List<LogEntry> messages)
    {
      this();
      this.messages = messages;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public Log_args(Log_args other) {
      if (other.isSetMessages()) {
        List<LogEntry> __this__messages = new ArrayList<LogEntry>();
        for (LogEntry other_element : other.messages) {
          __this__messages.add(new LogEntry(other_element));
        }
        this.messages = __this__messages;
      }
    }

    public Log_args deepCopy() {
      return new Log_args(this);
    }

    @Override
    public void clear() {
      this.messages = null;
    }

    public int getMessagesSize() {
      return (this.messages == null) ? 0 : this.messages.size();
    }

    public java.util.Iterator<LogEntry> getMessagesIterator() {
      return (this.messages == null) ? null : this.messages.iterator();
    }

    public void addToMessages(LogEntry elem) {
      if (this.messages == null) {
        this.messages = new ArrayList<LogEntry>();
      }
      this.messages.add(elem);
    }

    public List<LogEntry> getMessages() {
      return this.messages;
    }

    public Log_args setMessages(List<LogEntry> messages) {
      this.messages = messages;
      return this;
    }

    public void unsetMessages() {
      this.messages = null;
    }

    /** Returns true if field messages is set (has been assigned a value) and false otherwise */
    public boolean isSetMessages() {
      return this.messages != null;
    }

    public void setMessagesIsSet(boolean value) {
      if (!value) {
        this.messages = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case MESSAGES:
        if (value == null) {
          unsetMessages();
        } else {
          setMessages((List<LogEntry>)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case MESSAGES:
        return getMessages();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case MESSAGES:
        return isSetMessages();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof Log_args)
        return this.equals((Log_args)that);
      return false;
    }

    public boolean equals(Log_args that) {
      if (that == null)
        return false;

      boolean this_present_messages = true && this.isSetMessages();
      boolean that_present_messages = true && that.isSetMessages();
      if (this_present_messages || that_present_messages) {
        if (!(this_present_messages && that_present_messages))
          return false;
        if (!this.messages.equals(that.messages))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public int compareTo(Log_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      Log_args typedOther = (Log_args)other;

      lastComparison = Boolean.valueOf(isSetMessages()).compareTo(typedOther.isSetMessages());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetMessages()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.messages, typedOther.messages);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (field.id) {
          case 1: // MESSAGES
            if (field.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                this.messages = new ArrayList<LogEntry>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  LogEntry _elem2;
                  _elem2 = new LogEntry();
                  _elem2.read(iprot);
                  this.messages.add(_elem2);
                }
                iprot.readListEnd();
              }
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.messages != null) {
        oprot.writeFieldBegin(MESSAGES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, this.messages.size()));
          for (LogEntry _iter3 : this.messages)
          {
            _iter3.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("Log_args(");
      boolean first = true;

      sb.append("messages:");
      if (this.messages == null) {
        sb.append("null");
      } else {
        sb.append(this.messages);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

  }

  public static class Log_result implements org.apache.thrift.TBase<Log_result, Log_result._Fields>, java.io.Serializable, Cloneable   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Log_result");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.I32, (short)0);

    /**
     * 
     * @see ResultCode
     */
    public ResultCode success;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      /**
       * 
       * @see ResultCode
       */
      SUCCESS((short)0, "success");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments

    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ResultCode.class)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Log_result.class, metaDataMap);
    }

    public Log_result() {
    }

    public Log_result(
      ResultCode success)
    {
      this();
      this.success = success;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public Log_result(Log_result other) {
      if (other.isSetSuccess()) {
        this.success = other.success;
      }
    }

    public Log_result deepCopy() {
      return new Log_result(this);
    }

    @Override
    public void clear() {
      this.success = null;
    }

    /**
     * 
     * @see ResultCode
     */
    public ResultCode getSuccess() {
      return this.success;
    }

    /**
     * 
     * @see ResultCode
     */
    public Log_result setSuccess(ResultCode success) {
      this.success = success;
      return this;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((ResultCode)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return getSuccess();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return isSetSuccess();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof Log_result)
        return this.equals((Log_result)that);
      return false;
    }

    public boolean equals(Log_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public int compareTo(Log_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      Log_result typedOther = (Log_result)other;

      lastComparison = Boolean.valueOf(isSetSuccess()).compareTo(typedOther.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, typedOther.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (field.id) {
          case 0: // SUCCESS
            if (field.type == org.apache.thrift.protocol.TType.I32) {
              this.success = ResultCode.findByValue(iprot.readI32());
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("Log_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

  }

}
