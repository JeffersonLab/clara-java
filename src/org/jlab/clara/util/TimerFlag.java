package org.jlab.clara.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Every required seconds the flag will be up.
 * Created by gurjyan on 10/2/17.
 */
public class TimerFlag {
    private AtomicBoolean up = new AtomicBoolean(false);

    private Timer timer;

    public TimerFlag(int seconds){
        timer = new Timer();

        timer.scheduleAtFixedRate(new ChangeFlag(), 0, seconds * 1000);
    }

    public void reset(){
        up.set(false);
    }

    public boolean isUp(){
        return up.get();
    }

    private class ChangeFlag extends TimerTask {
        @Override
        public void run() {
            up.set(true);
        }
    }
}
