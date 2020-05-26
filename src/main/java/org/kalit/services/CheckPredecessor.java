package org.kalit.services;

import java.net.InetSocketAddress;

import org.kalit.Helper;
import org.kalit.chord.ChordNode;

public class CheckPredecessor implements Runnable {
    private ChordNode node;
    private boolean running;

    public CheckPredecessor(ChordNode node) {
        this.node = node;
        this.running = true;
    }

    @Override
    public void run() {
        while (running) {
            InetSocketAddress pred = node.getPredecessor();
            if (pred != null) {
                String response = Helper.sendRequest(pred, "KEEP");
                if (response == null || !response.equals("ALIVE")) {
                    node.setPredecessor(null);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        running = false;
    }
}