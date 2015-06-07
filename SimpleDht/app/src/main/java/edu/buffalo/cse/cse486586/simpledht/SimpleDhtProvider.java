package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.ViewGroup;
import android.view.inputmethod.CursorAnchorInfo;


public class SimpleDhtProvider extends ContentProvider {

    static String REMOTE_PORT0 = "11108";
    boolean wait = true;
    String predecessor = "null";
    String myPort = null;
    String successor = "null";
    public SQLiteDatabase database;
    private Context context;
    static final String TAG = "TAG";//SimpleDhtProvider.class.getSimpleName();
    Socket socket = null;
    static final int SERVER_PORT = 10000;
    List<String> portArrayList = new ArrayList<String>();
    Cursor finalCursor;
    private static final String KEY_FIELD = "key";


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d("Delete", "Inside delete" + selection);
        int row;
        row =  database.delete("MESSAGE_ORDER", "key=\"" + selection + "\"", null);
        Log.d("Delete", "After delete" + row);
        return row;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        if(predecessor.equals("null") && successor.equals("null")) {
            /*
            Block to insert for single AVD without predecessor and successor
             */
            database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.v("TAG insert", values.toString());
            Log.d("TAG VALUES", "Insert values : " + values);
        } else {
            try {

                String key = values.getAsString("key");
                String value = values.getAsString("value");
                String key_hash = key;//genHash(values.getAsString(KEY_FIELD));
                Log.d("TAG INSERT", "Insert request for key:"+key);
                if ((genHash(predecessor).compareTo(genHash(key_hash)) < 0) && (genHash(myPort).compareTo(genHash(key_hash)) >= 0)) {
                    /*
                    Block to determine position to insert for AVD when it is greater than predecessor and less than current node
                     */
                    Log.d("TAG INSERT","insert position in mid nodes");
                    database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                    Log.d("TAG INSERT", "Value inserted in the database" + values);

                } else if((genHash(myPort).compareTo(genHash(predecessor)) < 0 && genHash(myPort).compareTo(genHash(successor)) < 0)){
                    /*
                    Block to determine position to insert for AVD when it is greater than predecessor and greater than current node.
                    This situation happens when the position is at the end of the chord
                     */
                    Log.d("TAG INSERT","I am first Node in chord");
                    if((genHash(myPort).compareTo(genHash(key_hash)) > 0)){
                        Log.d("TAG INSERT","First Node. Between 0 - 1st node:"+myPort);
                        database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                        Log.d("TAG INSERT", "Value inserted in the database" + values);
                    } else if(((genHash(predecessor).compareTo(genHash(key_hash)) < 0) && (genHash(myPort).compareTo(genHash(key_hash)) < 0))){
                        Log.d("TAG INSERT","First Node. before 0:"+myPort);
                        database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                        Log.d("TAG INSERT", "Value inserted in the database" + values);
                    } else{
                        Log.e("TAG INSERT", "First Node. Unknown condition");
                        Log.d("TAG Insert Forward","Message Forwarded to successor:"+ successor+" msgkey:"+key);
                        Message insertSend = new Message(successor,successor);
                        insertSend.setKeyValue(key,value);
                        insertSend.setMsgType(MessageType.INSERT_REQUEST);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertSend);
                    }
                }
                else{
                    /*
                    Message forwarded to successor in case the position is not found between the nodes
                     */
                    Log.d("TAG Insert Forward","Message Forwarded to successor:"+ successor+" msgkey:"+key);
                    Message insertSend = new Message(successor,successor);
                    insertSend.setKeyValue(key,value);
                    insertSend.setMsgType(MessageType.INSERT_REQUEST);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertSend);
                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return uri;
    }

    private void insertMe(String key, String value) {
        ContentValues cv = new ContentValues();
        cv.put("key",key);
        cv.put("value",value);
        database.insertWithOnConflict(DBHelperClass.TABLE_NAME, "", cv, SQLiteDatabase.CONFLICT_REPLACE);
    }



    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Log.d(TAG, "Inside the onCreate method for creating database");
        DBHelperClass dbHelperClass = new DBHelperClass(getContext());
        database = dbHelperClass.getWritableDatabase();

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf(Integer.parseInt(portStr));
        ServerSocket serverSocket = null;
        Log.d(TAG, "MyPort ID: "+myPort);
        Message message = new Message(myPort, "5554");

        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (!(myPort.equals("5554"))) {
                /*
                Block to check for non-5554 nodes and direct them to monitor node 5554 for join request in the chord
                 */
                Log.d(TAG, "Array contains " + portArrayList);
                message.setMsgType(MessageType.FROM_ON_CREATE_MESSAGE);
                message.setToSend("5554");
                Log.d(TAG, "Inside the onCreate for non 5554 ports");
                Log.d("TAG Client port to be sent ", message.getToSend());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            } else {
                portArrayList.add("5554");
                Log.d(TAG, "Inside the onCreate for 5554 port");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("MESSAGE_ORDER");
        Cursor c3 = null;

        Log.v("TAG query", selection);
        Log.d(TAG, "Selection statement " + selection);
        Log.d(TAG, "Before the query if condition statement");
        if(predecessor.equals("null") && successor.equals("null")) {
            Cursor c2;
            /*
            Block to query for single AVD with * and @ and with selection key
             */
            if ((selection.equals("*")) || (selection.equals("@")) || (selection.equals("\"*\"")) || (selection.equals("\"@\""))) {
                c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
                Log.d(TAG, "Inside the query for local dump with single AVD");
                c2.moveToFirst();
            }
            else {
                c2 = sqLiteQueryBuilder.query(database, null,  "key=\"" + selection + "\"", null, null, null, null);
                Log.d(TAG, "Inside the query for local dump with single AVD");
                c2.moveToFirst();
            }
            return c2;
        } else {
            if ((selection.equals("@")) || (selection.equals("\"@\""))) {
                /*
                Local dump of the database for @ query
                 */
                Log.d(TAG, "Inside the query end else condition statement for @ condition");
                Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
                c2.moveToFirst();
                return c2;
            } else if ((selection.equals("*")) || (selection.equals("\"*\""))) {
                /*
                Global dump of the database for * query
                 */
                Log.d(TAG, "Inside the query end else condition statement for * condition");
                Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
                c2.moveToFirst();
                ArrayList<String> query_array = new ArrayList<String>();
                while (!(c2.isAfterLast())) {
                    query_array.add(c2.getString(0));
                    query_array.add(c2.getString(1));
                    c2.moveToNext();
                }
                Message mess_query_array = new Message(successor, successor);
                mess_query_array.message_array = query_array;
                mess_query_array.setMsgType(MessageType.QUERY_ALL);
                mess_query_array.setOriginalSender(myPort);
                mess_query_array.setQueryKey("*");
                Log.d(TAG, "first array list size " + mess_query_array.message_array.size());

                ClientTask tempTask = new ClientTask();
                tempTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array);
                try {
                    String st = tempTask.get();
                    Log.d("Final cursor count", "Final cursor count: " + finalCursor.getCount());
                    c2 = finalCursor;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                return c2;
            } else {
                /*
                Query for selection parameter
                 */
                Cursor c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
                c1.moveToFirst();
                if (c1.getCount() == 0) {
                    Log.d("QUERY", "Empty query in IF loop for key: " + selection);
                    Message mess_query = new Message(successor, successor);
                    mess_query.setQueryKey(selection);
                    mess_query.setMsgType(MessageType.QUERY_INIT_REQUEST);
                    mess_query.setOriginalSender(myPort);
                    Log.d("QUERY", "Query request forwarded to node: " + successor + " for key : " + selection + " from:" + mess_query.getOriginalSender());

                    ClientTask tempTask = new ClientTask();
                    tempTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query);
                    try {
                        String st = tempTask.get();
                        c1 = finalCursor;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    Log.d("QUERY", "Non empty query in IF loop for key:" + selection);
                    Log.d("QUERY", "Query found in node: " + myPort + "for key : " + selection);
                }
                return c1;
            }
        }
    }

    private Cursor query(String selection, String originalSender) {
        /*
        Block to forward query to successor node in case the data is not present in the curretn chord space
         */
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("MESSAGE_ORDER");
        Cursor c1 = sqLiteQueryBuilder.query(database, null, "key=\"" + selection + "\"", null, null, null, null);
        c1.moveToFirst();
        if (c1.getCount() == 0) {
            Log.d("QUERY", "Empty query in IF loop for key: " + selection);
            Message mess_query = new Message(successor, successor);
            mess_query.setQueryKey(selection);
            mess_query.setMsgType(MessageType.QUERY_REQUEST);
            mess_query.setOriginalSender(originalSender);
            Log.d("QUERY", "Query request forwarded to node: " + successor + " for key : " + selection);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query);
            return null;
        }
        return c1;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ObjectInputStream objectInputStream = null;

            while (!serverSocket.isClosed()) {
                try {
                    socket = serverSocket.accept();
                    objectInputStream = new ObjectInputStream(socket.getInputStream());
                    Message m = (Message) objectInputStream.readObject();
                    Log.d(TAG, "Inside the serverTask method");
                    Log.d("TAG CHORD RING LIST", "portArrayList contains: " + portArrayList);
                    publishProgress(m);
                } catch (IOException e) {
                    Log.d(TAG, "Socket IOException at server end");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        protected void onProgressUpdate(Message... messages) {
            Message m1 = messages[0];
            int flag1;
            int i = 0;

            if (m1.getMsgType().equals(MessageType.FROM_ON_CREATE_MESSAGE)) {
                Log.d("TAG JOIN_REQUEST", m1.getSender());
                try {
                    if (portArrayList.size() == 1) {
                        /*
                        Block to join a node in chord space of 1
                         */
                        Log.d("TAG JOIN_REQUEST", "Join request for single node in chord" + portArrayList);
                        flag1 = genHash(portArrayList.get(0)).compareTo(genHash(m1.getSender()));
                        if (flag1 > 0) {
                            portArrayList.add(0, m1.getSender());
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in if loop" + portArrayList);
                            Log.d("TAG JOIN_REQUEST", "Port added in the chord list" + portArrayList);

                            Message m8 = new Message(portArrayList.get(0), portArrayList.get(0));
                            m8.setPredecessor(portArrayList.get(1));
                            m8.setSuccessor(portArrayList.get(1));
                            m8.setMsgType(MessageType.SET_BOTH_NODES);
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in if loop" + portArrayList);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m8);

                            Message m9 = new Message(portArrayList.get(1), portArrayList.get(1));
                            m9.setPredecessor(portArrayList.get(0));
                            m9.setSuccessor(portArrayList.get(0));
                            m9.setMsgType(MessageType.SET_BOTH_NODES);
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in if loop" + portArrayList);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m9);

                        } else {
                            portArrayList.add(1, m1.getSender());
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in else loop" + portArrayList);
                            Log.d("TAG JOIN_REQUEST", "Port added in the chord list" + portArrayList);

                            Message m10 = new Message(portArrayList.get(1), portArrayList.get(1));
                            m10.setPredecessor(portArrayList.get(0));
                            m10.setSuccessor(portArrayList.get(0));
                            m10.setMsgType(MessageType.SET_BOTH_NODES);
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in if loop" + portArrayList);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m10);

                            Message m11 = new Message(portArrayList.get(0), portArrayList.get(0));
                            m11.setPredecessor(portArrayList.get(1));
                            m11.setSuccessor(portArrayList.get(1));
                            m11.setMsgType(MessageType.SET_BOTH_NODES);
                            Log.d("TAG JOIN_REQUEST", "Join request for single node in chord in if loop" + portArrayList);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m11);

                        }
                    } else if (genHash(portArrayList.get(0)).compareTo(genHash(m1.getSender())) > 0) {
                        /*
                            Block to join a node in chord space of 1 at the first position
                        */

                        portArrayList.add(0, m1.getSender());

                        Log.d("TAG JOIN_FIRST NODE", "JOIN_REQUEST FOR FIRST NODE before the msgs being sent");
                        Log.d("TAG JOIN_REQUEST", "Port added in the chord list" + portArrayList);

                        Message m5 = new Message(portArrayList.get(portArrayList.size()-1),portArrayList.get(portArrayList.size()-1));
                        m5.setMsgType(MessageType.SET_NEW_SUCCESSOR);
                        Log.d("TAG Client port to be sent ", m5.getToSend());
                        Log.d("TAG JOIN REQUEST", "List for chord: " + portArrayList);
                        m5.setSuccessor(portArrayList.get(0));
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m5);

                        Message m6 = new Message(portArrayList.get((1)),portArrayList.get((1)));
                        m6.setMsgType(MessageType.SET_NEW_PREDECESSOR);
                        m6.setPredecessor(portArrayList.get((0)));
                        Log.d("TAG JOIN FIRST NODE", "inside set new predecessor block" + m6.getToSend() + m6.getMsgType());
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m6);

                        Message m7 = new Message(portArrayList.get((0)),portArrayList.get((0)));
                        m7.setMsgType(MessageType.SET_BOTH_NODES);
                        m7.setPredecessor(portArrayList.get(portArrayList.size()-1));
                        m7.setSuccessor(portArrayList.get((1)));
                        Log.d("TAG JOIN FIRST NODE", "inside set new predecessor block" + m7.getToSend() + m7.getMsgType());
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m7);

                    } else if (portArrayList.size() >= 2) {
                        /*
                        Block to join a node in chord space when it is greater than predecessor and less than successor
                         */
                        int temp = -1;
                        for (i = 0; i < portArrayList.size()-1; i++) {
                            Log.d("TAG JOIN MIDDLE NODE", "sender of the message is : " + m1.getSender() + "with array of size " + portArrayList.size());
                            int flag2 = genHash(portArrayList.get(i)).compareTo(genHash(m1.getSender()));
                            if (flag2 < 0) {
                                int flag3 = genHash(portArrayList.get(i + 1)).compareTo(genHash(m1.getSender()));
                                {
                                    if (flag3 > 0) {
                                        temp = i+1;
                                    }
                                }
                            }
                        }

                        if (temp == -1) {
                            portArrayList.add(m1.getSender());
                            temp = portArrayList.size()-1;
                            Message m2 = new Message(portArrayList.get(temp-1),portArrayList.get(temp-1));
                            m2.setMsgType(MessageType.SET_NEW_SUCCESSOR);
                            m2.setSuccessor(portArrayList.get((temp) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m2.getToSend());
                            Log.d("TAG SUCCESSOR", "type" + m2.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m2);

                            Message m3 = new Message(portArrayList.get((temp+1) % portArrayList.size()),portArrayList.get((temp+1) % portArrayList.size()));
                            m3.setMsgType(MessageType.SET_NEW_PREDECESSOR);
                            m3.setPredecessor(portArrayList.get((temp) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m3.getToSend());
                            Log.d("TAG PREDECESSOR", "type" + m3.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m3);

                            Message m4 = new Message(portArrayList.get((temp) % portArrayList.size()),portArrayList.get((temp) % portArrayList.size()));
                            m4.setMsgType(MessageType.SET_BOTH_NODES);
                            m4.setPredecessor(portArrayList.get(temp-1));
                            m4.setSuccessor(portArrayList.get((temp + 1) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m4.getToSend());
                            Log.d("TAG BOTH NODES", "type" + m4.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m4);


                        } else {
                            portArrayList.add(((temp)), m1.getSender());
                            Log.d("TAG JOIN_REQUEST", "Port added in the chord list" + portArrayList);
                            Message m2 = new Message(portArrayList.get(temp+1),portArrayList.get(temp+1));
                            m2.setMsgType(MessageType.SET_NEW_PREDECESSOR);
                            m2.setPredecessor(portArrayList.get((temp) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m2.getToSend());
                            Log.d("TAG SUCCESSOR", "type" + m2.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m2);

                            Message m3 = new Message(portArrayList.get((temp-1) % portArrayList.size()),portArrayList.get((temp-1) % portArrayList.size()));
                            m3.setMsgType(MessageType.SET_NEW_SUCCESSOR);
                            m3.setSuccessor(portArrayList.get((temp) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m3.getToSend());
                            Log.d("TAG PREDECESSOR", "type" + m3.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m3);

                            Message m4 = new Message(portArrayList.get((temp) % portArrayList.size()),portArrayList.get((temp) % portArrayList.size()));
                            m4.setMsgType(MessageType.SET_BOTH_NODES);
                            m4.setPredecessor(portArrayList.get(temp-1));
                            m4.setSuccessor(portArrayList.get((temp+1) % portArrayList.size()));
                            Log.d("TAG Client port to be sent ", m4.getToSend());
                            Log.d("TAG BOTH NODES", "type" + m4.getMsgType());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m4);

                        }


                    }
                } catch(NoSuchAlgorithmException e){
                    e.printStackTrace();
                }
            } else if (m1.getMsgType().equals(MessageType.SET_NEW_PREDECESSOR)) {
                predecessor = m1.getPredecessor();
                Log.d("TAG PREDESS", "Current Predecessor" + predecessor);
                Log.d("TAG PREDESS", "Current Successor" + successor);


            } else if (m1.getMsgType().equals(MessageType.SET_NEW_SUCCESSOR)) {
                successor = m1.getSuccessor();
                Log.d("TAG SUCCESSOR", "Current Predecessor" + predecessor);
                Log.d("TAG SUCCESSOR", "Current Successor" + successor);


            } else if (m1.getMsgType().equals(MessageType.SET_BOTH_NODES)) {
                predecessor = m1.getPredecessor();
                successor = m1.getSuccessor();
                Log.d("TAG BOTH NODES", "Current Predecessor" + predecessor);
                Log.d("TAG BOTH NODES", "Current Successor" + successor);

            } else if(m1.getMsgType().equals(MessageType.INSERT_REQUEST)){
                Log.d("TAG INSERT REQUEST REC","from:"+predecessor+" key:"+m1.getKey());
                insert(m1.getKey(), m1.getValue());

            } else if(m1.getMsgType().equals(MessageType.QUERY_REQUEST) || m1.getMsgType().equals(MessageType.QUERY_INIT_REQUEST)) {
                /*
                Query request to be forwarded to successor in case data is not found
                 */
                Log.d("TAG QUERY REQUEST REC", "from:" + myPort + " key:" + m1.getQuery_key()+ " origin:"+m1.getOriginalSender());
                Cursor c = query(m1.getQuery_key(),m1.getOriginalSender());
                if(c!=null){
                    Log.d("TAG QUERY FOUND","Sending to original sender:"+m1.getOriginalSender()+" key:"+m1.getQuery_key());
                    c.moveToFirst();
                    m1.setKeyValue(c.getString(c.getColumnIndex(KEY_FIELD)),c.getString(c.getColumnIndex("value")));
                    m1.setMsgType(MessageType.QUERY_FOUND);
                    m1.setToSend(m1.getOriginalSender());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1);
                }
            } else if(m1.getMsgType().equals(MessageType.QUERY_FOUND)){
                Log.d("TAG QUERY FOUND","QUERY FOUND AND RECEIVED key:"+m1.getKey());
                insertMe(m1.getKey(), m1.getValue());
                finalCursor = query(null,null,m1.getKey(),null,null);
                delete(null,m1.getKey(),null);
                wait = false;

            } else if (m1.getMsgType().equals(MessageType.QUERY_ALL)) {
                /*
                Query block for * selection
                 */
                SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
                sqLiteQueryBuilder.setTables("MESSAGE_ORDER");

                if (!(m1.getOriginalSender().equals(myPort))) {
                    Log.d(TAG, "Inside the query end else condition statement for * condition");
                    Cursor c2 = sqLiteQueryBuilder.query(database, null, null, null, null, null, null);
                    c2.moveToFirst();
                    ArrayList<String> query_array = m1.message_array;
                    while(!(c2.isAfterLast())) {
                        query_array.add(c2.getString(0));
                        query_array.add(c2.getString(1));
                        c2.moveToNext();
                    }
                    Message mess_query_array = new Message(successor, successor);
                    mess_query_array.setMsgType(MessageType.QUERY_ALL);
                    mess_query_array.setOriginalSender(m1.getOriginalSender());
                    mess_query_array.setQueryKey("*");
                    mess_query_array.message_array = query_array;
                    Log.d(TAG, "first array list size " + mess_query_array.message_array.size());

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess_query_array);
                } else {
                    int f=0;
                    Log.d("FINAL ARRAY", "Final array list size" + m1.message_array.size());
                    MatrixCursor matrixCursor = new MatrixCursor((new String[]{"key", "value"}));
                    while(f<m1.message_array.size()) {
                        matrixCursor.addRow(new String[]{m1.message_array.get(f),m1.message_array.get(f+1)});
                        f=f+2;
                    }
                    finalCursor = matrixCursor;
                    Log.d("Cursor count","Cursor count: " + matrixCursor.getCount());
                    wait=false;

                }
            }

        }
    }



    private void insert(String key, String value) {
        ContentValues cv = new ContentValues();
        cv.put("key", key);
        cv.put("value",value);
        insert(null, cv);
    }


    private class ClientTask extends AsyncTask<Message, Void, String> {


        protected String doInBackground(Message... msgs) {
            Message msgToSend = msgs[0];
            Socket socket;
            String remotePort;
            ObjectOutputStream objectOutputStream;
            Log.d("TAG client task", "Inside the clientTask method");
            Log.d("TAG client task", "Inside the clientTask method for msg type " + msgToSend.getMsgType());

            try {
                remotePort = msgToSend.getToSend();
                Log.d("TAG client task", "Inside the clientTask method for port " + remotePort);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(remotePort)*2));
                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(msgToSend);
                objectOutputStream.flush();
                objectOutputStream.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.d(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.d(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            if(msgToSend.getMsgType().equals(MessageType.QUERY_INIT_REQUEST)){
                /*
                Wait block for original sender to retrieve the messages
                 */
                Log.d("TAG WAIT","START");
                wait = true;
                while(wait){
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Log.d("TAG WAIT","OVER");

            }
            if((msgToSend.getMsgType().equals(MessageType.QUERY_ALL)) && msgToSend.getOriginalSender().equals(myPort)){
                /*
                Wait block for original sender for * selection
                 */
                Log.d("TAG WAIT","START");
                wait = true;
                while(wait){
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Log.d("TAG WAIT","OVER");

            }
            return null;
        }
    }
}

