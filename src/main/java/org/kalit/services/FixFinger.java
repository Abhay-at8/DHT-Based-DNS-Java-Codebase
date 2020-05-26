package org.kalit.services;

import java.net.InetSocketAddress;
import java.util.Random;

import org.kalit.Helper;
import org.kalit.chord.ChordNode;

public class FixFinger implements Runnable {
    private ChordNode node;
    private boolean running;
    Random random;

    public FixFinger(ChordNode node) {
        this.node = node;
        this.running = true;
        random = new Random();
    }

    @Override
    public void run() {
        while (running) {
            int next = random.nextInt(31) + 2;
            InetSocketAddress nextFinger = node.findSuccessor(Helper.ithStart(node.getId(), next));
            node.updateFingers(next, nextFinger);
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