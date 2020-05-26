package org.kalit.services;

import java.net.InetSocketAddress;

import org.kalit.Helper;
import org.kalit.chord.ChordNode;
import org.kalit.chord.HashFunction;

public class Stabilize implements Runnable {
    private ChordNode node;
    private boolean running;

    public Stabilize(ChordNode node) {
        this.node = node;
        this.running = true;
    }

    @Override
    public void run() {
        while (running) {
            InetSocketAddress successor = node.getSuccessor();
            if (successor == null || successor.equals(node.getNodeAddress())) {
                node.updateFingers(-3, null);
            }
            successor = node.getSuccessor();
            if (successor != null && !successor.equals(node.getNodeAddress())) {
                InetSocketAddress x = Helper.requestAddress(successor, "YOURPRE");

                if (x == null) {
                    node.updateFingers(-1, null);
                } else if (!x.equals(successor)) {
                    long nodeId = node.getId();
                    long successorId = HashFunction.hashSocketAddress(successor);
                    long xId = HashFunction.hashSocketAddress(x);

                    long successorRelativeId = Helper.computeRelativeId(successorId, nodeId);
                    long xRelativeId = Helper.computeRelativeId(xId, nodeId);

                    if (xRelativeId > 0 && xRelativeId < successorRelativeId) {
                        node.updateFingers(1, x);
                    }
                } else {
                    node.notify(successor);
                }
            }
            try {
                Thread.sleep(60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        running = false;
    }
}