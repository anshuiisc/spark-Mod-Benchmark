package Topology;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anshushukla on 18/06/15.
 */
public class MsgIdAddandRemove implements Serializable {

    public static  List<String> addMessageId(List<String> tuple,long  msgid)
    {
    List<String>        tuple1=new ArrayList<String>();


        for(String s : tuple) {

            tuple1.add(new StringBuffer(String.valueOf(msgid)).append("@").append(s).toString());

        }return tuple1;
    }



    public static String addMessageId(String tuple,long msgid)
    {
        return new StringBuffer(String.valueOf(msgid)).append("@").append(tuple).toString();
    }



    public static long getMessageId(String tuple)
    {
        return Long.parseLong(tuple.split("@")[0]);
    }

    public static String getMessageContent(String tuple)
    {
        return tuple.split("@")[1];

    }


public static String  getexecutorname() throws UnknownHostException {

    return InetAddress.getLocalHost().getHostName();
}

}
