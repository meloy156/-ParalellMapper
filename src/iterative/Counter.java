package iterative;

public final class Counter {

    private int remainig;
    private boolean done;

    Counter(int size) {
        remainig = size;
        done = false;
    }

    public synchronized void decrement() {
        if (done) return;
        remainig--;
        if (remainig == 0) {
            done = true;
            notifyAll();
        }
    }

    public synchronized void await() throws InterruptedException {
        while (!done) {
            wait();
        }
    }
}
