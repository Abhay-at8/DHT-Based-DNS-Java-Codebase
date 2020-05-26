package org.kalit.chord;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.kalit.Helper;
import org.kalit.services.CheckPredecessor;
import org.kalit.services.DNSLookUp;
import org.kalit.services.FixFinger;
import org.kalit.services.Listener;
import org.kalit.services.Stabilize;

public class ChordNode {

    private long nodeId;
    private InetSocketAddress nodeAddress;
    private InetSocketAddress predecessor;
    private Map<Integer, InetSocketAddress> fingerTable;
    private Listener listener;
    private Stabilize stabilize;
    private FixFinger fixFingers;
    private CheckPredecessor checkPredecessor;
    private DNSLookUp dnsLookUp;

    public ChordNode(InetSocketAddress address) {
        this.nodeAddress = address;
        this.nodeId = HashFunction.hashSocketAddress(nodeAddress);
        fingerTable = new HashMap<Integer, InetSocketAddress>();
        for (int i = 1; i <= 32; i++) {
            updateIthFinger(i, null);
        }
        predecessor = null;

        listener = new Listener(this);
        stabilize = new Stabilize(this);
        fixFingers = new FixFinger(this);
        checkPredecessor = new CheckPredecessor(this);
        dnsLookUp = new DNSLookUp(this);
    }

    @Override
    public String toString() {
        return "Server:" + nodeAddress.getPort();
    }

    public boolean join(InetSocketAddress contact) {
        if (contact != null && !contact.equals(nodeAddress)) {
            InetSocketAddress successor = Helper.requestAddress(contact, "FINDSUCC_" + nodeId);
            if (successor == null) {
                System.out.println("\nCannot find node you are trying to contact. Please exit.\n");
                return false;
            }
            updateIthFinger(1, successor);
        }

        new Thread(listener).start();
        new Thread(stabilize).start();
        new Thread(fixFingers).start();
        new Thread(checkPredecessor).start();
        new Thread(dnsLookUp).start();
        return true;
    }

    public String notify(InetSocketAddress successor) {
        if (successor != null && !successor.equals(nodeAddress))
            return Helper.sendRequest(successor,
                    "IAMPRE_" + nodeAddress.getAddress().toString() + ":" + nodeAddress.getPort());
        else
            return null;
    }

    public void notified(InetSocketAddress newpre) {
        if (predecessor == null || predecessor.equals(nodeAddress)) {
            this.setPredecessor(newpre);
        } else {
            long oldpre_id = HashFunction.hashSocketAddress(predecessor);
            long local_relative_id = Helper.computeRelativeId(nodeId, oldpre_id);
            long newpre_relative_id = Helper.computeRelativeId(HashFunction.hashSocketAddress(newpre), oldpre_id);
            if (newpre_relative_id > 0 && newpre_relative_id < local_relative_id)
                this.setPredecessor(newpre);
        }
    }

    public InetSocketAddress findSuccessor(long id) {

        InetSocketAddress succ = this.getSuccessor();
        InetSocketAddress pre = findPredecessor(id);

        if (!pre.equals(nodeAddress))
            succ = Helper.requestAddress(pre, "YOURSUCC");

        if (succ == null)
            succ = nodeAddress;

        return succ;
    }

    private InetSocketAddress findPredecessor(long findid) {
        InetSocketAddress n = this.nodeAddress;
        InetSocketAddress n_successor = this.getSuccessor();
        InetSocketAddress most_recently_alive = this.nodeAddress;
        long n_successor_relative_id = 0;
        if (n_successor != null)
            n_successor_relative_id = Helper.computeRelativeId(HashFunction.hashSocketAddress(n_successor),
                    HashFunction.hashSocketAddress(n));
        long findid_relative_id = Helper.computeRelativeId(findid, HashFunction.hashSocketAddress(n));

        while (!(findid_relative_id > 0 && findid_relative_id <= n_successor_relative_id)) {
            InetSocketAddress pre_n = n;
            if (n.equals(this.nodeAddress)) {
                n = this.closest_preceding_finger(findid);
            } else {
                InetSocketAddress result = Helper.requestAddress(n, "CLOSEST_" + findid);

                if (result == null) {
                    n = most_recently_alive;
                    n_successor = Helper.requestAddress(n, "YOURSUCC");
                    if (n_successor == null) {
                        System.out.println("It's not possible.");
                        return nodeAddress;
                    }
                    continue;
                } else if (result.equals(n))
                    return result;
                else {
                    most_recently_alive = n;
                    n_successor = Helper.requestAddress(result, "YOURSUCC");
                    if (n_successor != null) {
                        n = result;
                    } else {
                        n_successor = Helper.requestAddress(n, "YOURSUCC");
                    }
                }

                n_successor_relative_id = Helper.computeRelativeId(HashFunction.hashSocketAddress(n_successor),
                        HashFunction.hashSocketAddress(n));
                findid_relative_id = Helper.computeRelativeId(findid, HashFunction.hashSocketAddress(n));
            }
            if (pre_n.equals(n))
                break;
        }
        return n;
    }

    public InetSocketAddress closest_preceding_finger(long findid) {
        long findid_relative = Helper.computeRelativeId(findid, nodeId);

        // check from last item in finger table
        for (int i = 32; i > 0; i--) {
            InetSocketAddress ith_finger = fingerTable.get(i);
            if (ith_finger == null) {
                continue;
            }
            long ith_finger_id = HashFunction.hashSocketAddress(ith_finger);
            long ith_finger_relative_id = Helper.computeRelativeId(ith_finger_id, nodeId);

            if (ith_finger_relative_id > 0 && ith_finger_relative_id < findid_relative) {
                String response = Helper.sendRequest(ith_finger, "KEEP");

                if (response != null && response.equals("ALIVE")) {
                    return ith_finger;
                } else {
                    updateFingers(-2, ith_finger);
                }
            }
        }
        return nodeAddress;
    }

    public synchronized void updateFingers(int i, InetSocketAddress value) {

        if (i > 0 && i <= 32) {
            updateIthFinger(i, value);
        } else if (i == -1) {
            deleteSuccessor();
        } else if (i == -2) {
            deleteCertainFinger(value);
        } else if (i == -3) {
            fillSuccessor();
        }
    }

    private void updateIthFinger(int i, InetSocketAddress value) {
        fingerTable.put(i, value);
        if (i == 1 && value != null && !value.equals(nodeAddress)) {
            notify(value);
        }
    }

    private void deleteSuccessor() {
        InetSocketAddress successor = getSuccessor();

        if (successor == null)
            return;

        int i = 32;
        for (i = 32; i > 0; i--) {
            InetSocketAddress ithfinger = fingerTable.get(i);
            if (ithfinger != null && ithfinger.equals(successor))
                break;
        }

        for (int j = i; j >= 1; j--) {
            updateIthFinger(j, null);
        }

        if (predecessor != null && predecessor.equals(successor))
            setPredecessor(null);

        fillSuccessor();
        successor = getSuccessor();

        if ((successor == null || successor.equals(successor)) && predecessor != null
                && !predecessor.equals(nodeAddress)) {
            InetSocketAddress p = predecessor;
            InetSocketAddress p_pre = null;
            while (true) {
                p_pre = Helper.requestAddress(p, "YOURPRE");
                if (p_pre == null)
                    break;

                if (p_pre.equals(p) || p_pre.equals(nodeAddress) || p_pre.equals(successor)) {
                    break;
                } else {
                    p = p_pre;
                }
            }
            updateIthFinger(1, p);
        }
    }

    private void deleteCertainFinger(InetSocketAddress f) {
        for (int i = 32; i > 0; i--) {
            InetSocketAddress ithfinger = fingerTable.get(i);
            if (ithfinger != null && ithfinger.equals(f))
                fingerTable.put(i, null);
        }
    }

    private void fillSuccessor() {
        InetSocketAddress successor = this.getSuccessor();
        if (successor == null || successor.equals(nodeAddress)) {
            for (int i = 2; i <= 32; i++) {
                InetSocketAddress ithfinger = fingerTable.get(i);
                if (ithfinger != null && !ithfinger.equals(nodeAddress)) {
                    for (int j = i - 1; j >= 1; j--) {
                        updateIthFinger(j, ithfinger);
                    }
                    break;
                }
            }
        }
        successor = getSuccessor();
        if ((successor == null || successor.equals(nodeAddress)) && predecessor != null
                && !predecessor.equals(nodeAddress)) {
            updateIthFinger(1, predecessor);
        }
    }

    public synchronized void setPredecessor(InetSocketAddress pre) {
        predecessor = pre;
    }

    public long getId() {
        return this.nodeId;
    }

    public InetSocketAddress getNodeAddress() {
        return this.nodeAddress;
    }

    public InetSocketAddress getPredecessor() {
        return this.predecessor;
    }

    public InetSocketAddress getSuccessor() {
        if (fingerTable != null && fingerTable.size() > 0) {
            return fingerTable.get(1);
        }
        return null;
    }

    public void printNeighbors() {
        System.out.println("\nYou are listening on port " + nodeAddress.getPort() + "." + "\nYour position is "
                + Helper.hexIdAndPosition(nodeAddress) + ".");
        InetSocketAddress successor = fingerTable.get(1);

        if ((predecessor == null || predecessor.equals(nodeAddress))
                && (successor == null || successor.equals(nodeAddress))) {
            System.out.println("Your predecessor is yourself.");
            System.out.println("Your successor is yourself.");
        } else {
            if (predecessor != null) {
                System.out.println("Your predecessor is node " + predecessor.getAddress().toString() + ", " + "port "
                        + predecessor.getPort() + ", position " + Helper.hexIdAndPosition(predecessor) + ".");
            } else {
                System.out.println("Your predecessor is updating.");
            }

            if (successor != null) {
                System.out.println("Your successor is node " + successor.getAddress().toString() + ", " + "port "
                        + successor.getPort() + ", position " + Helper.hexIdAndPosition(successor) + ".");
            } else {
                System.out.println("Your successor is updating.");
            }
        }
    }

    public void printDataStructure() {
        System.out.println("\n==============================================================");
        System.out.println("\nLOCAL:\t\t\t\t" + nodeAddress.toString() + "\t" + Helper.hexIdAndPosition(nodeAddress));
        if (predecessor != null)
            System.out.println(
                    "\nPREDECESSOR:\t\t\t" + predecessor.toString() + "\t" + Helper.hexIdAndPosition(predecessor));
        else
            System.out.println("\nPREDECESSOR:\t\t\tNULL");
        System.out.println("\nFINGER TABLE:\n");
        for (int i = 1; i <= 32; i++) {
            long ithstart = Helper.ithStart(HashFunction.hashSocketAddress(nodeAddress), i);
            InetSocketAddress f = fingerTable.get(i);
            StringBuilder sb = new StringBuilder();
            sb.append(i + "\t" + Helper.longTo8DigitHex(ithstart) + "\t\t");
            if (f != null)
                sb.append(f.toString() + "\t" + Helper.hexIdAndPosition(f));
            else
                sb.append("NULL");
            System.out.println(sb.toString());
        }
        System.out.println("\n==============================================================\n");
    }

    public void stopAllThreads() {
        if (listener != null)
            listener.stop();
        if (fixFingers != null)
            fixFingers.stop();
        if (stabilize != null)
            stabilize.stop();
        if (checkPredecessor != null)
            checkPredecessor.stop();
        if (dnsLookUp != null)
            dnsLookUp.stop();
    }
}