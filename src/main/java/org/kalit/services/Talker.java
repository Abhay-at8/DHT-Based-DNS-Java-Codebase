package org.kalit.services;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.kalit.Helper;
import org.kalit.chord.ChordNode;

public class Talker implements Runnable {

    Socket talkSocket;
    private ChordNode node;

    public Talker(Socket talkSocket, ChordNode node) {
        this.talkSocket = talkSocket;
        this.node = node;
    }

    public void run() {
        InputStream input = null;
        OutputStream output = null;
        try {
            input = talkSocket.getInputStream();
            String request = Helper.inputStreamToString(input);
            String response = processRequest(request);
            if (response != null) {
                output = talkSocket.getOutputStream();
                output.write(response.getBytes());
            }
            input.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot talk.\nServer port: " + node.getNodeAddress().getPort()
                    + "; Talker port: " + talkSocket.getPort(), e);
        }
    }

    private String processRequest(String request) {
        InetSocketAddress result = null;
        String ret = null;
        if (request == null) {
            return null;
        }
        if (request.startsWith("CLOSEST")) {
            long id = Long.parseLong(request.split("_")[1]);
            result = node.closest_preceding_finger(id);
            String ip = result.getAddress().toString();
            int port = result.getPort();
            ret = "MYCLOSEST_" + ip + ":" + port;
        } else if (request.startsWith("YOURSUCC")) {
            result = node.getSuccessor();
            if (result != null) {
                String ip = result.getAddress().toString();
                int port = result.getPort();
                ret = "MYSUCC_" + ip + ":" + port;
            } else {
                ret = "NOTHING";
            }
        } else if (request.startsWith("YOURPRE")) {
            result = node.getPredecessor();
            if (result != null) {
                String ip = result.getAddress().toString();
                int port = result.getPort();
                ret = "MYPRE_" + ip + ":" + port;
            } else {
                ret = "NOTHING";
            }
        } else if (request.startsWith("FINDSUCC")) {
            long id = Long.parseLong(request.split("_")[1]);
            result = node.findSuccessor(id);
            String ip = result.getAddress().toString();
            int port = result.getPort();
            ret = "FOUNDSUCC_" + ip + ":" + port;
        } else if (request.startsWith("IAMPRE")) {
            InetSocketAddress new_pre = Helper.createSocketAddress(request.split("_")[1]);
            node.notified(new_pre);
            ret = "NOTIFIED";
        } else if (request.startsWith("KEEP")) {
            ret = "ALIVE";
        }
        return ret;
    }
}