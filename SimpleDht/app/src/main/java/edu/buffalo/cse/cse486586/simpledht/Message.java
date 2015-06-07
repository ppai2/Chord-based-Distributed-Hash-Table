package edu.buffalo.cse.cse486586.simpledht;


import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * Created by prasan on 3/31/15.
 */
public class Message implements Serializable{
    private String sender;
    private String predecessor;
    private String successor;
    private String originalSender;
    private MessageType msgType;
    String toSend;
    private String key,value;
    String query_key;
    ArrayList<String> message_array;

    Message(String sender, String toSend) {
        this.sender = sender;
        this.toSend = toSend;
        msgType = MessageType.START;
    }

    public void setKeyValue(String key,String value){
        this.key = key;
        this.value = value;
    }

    public String getKey(){
        return key;
    }

    public String getValue(){
        return value;
    }

    public void setOriginalSender(String port){
        originalSender = port;
    }

    public String getOriginalSender(){
        return originalSender;
    }

    public void setQueryKey(String query_key) {
        this.query_key = query_key;
    }

    public String getQuery_key() {
        return query_key;
    }

    public void setMsgType(MessageType type) {
        this.msgType = type;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setToSend(String toSend) {
        this.toSend = toSend;
    }

    public String getToSend() {
        return toSend;
    }

    public void setPredecessor(String pre) {
        this.predecessor = pre;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSender() {
        return sender;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getSuccessor() {
        return successor;
    }
}



enum MessageType {
    START, FROM_ON_CREATE_MESSAGE, SET_NEW_PREDECESSOR, SET_NEW_SUCCESSOR, SET_BOTH_NODES, INSERT_REQUEST, QUERY_INIT_REQUEST, QUERY_REQUEST, QUERY_FOUND, QUERY_ALL
}