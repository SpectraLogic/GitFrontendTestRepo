package com.spectralogic.util.testfrmwrk;

import com.spectralogic.util.lang.Duration;

public class TestPerfMonitor {
    public synchronized static void hit(String event) {
        System.out.println(event + " after " + formatDuration(DURATION.getElapsedMillis()));
        DURATION = new Duration();
    }
    private static Duration DURATION = new Duration();

    private static String formatDuration(long millis) {
        long seconds = millis / 1000;
        long milliseconds = millis % 1000;
        long minutes = seconds / 60;
        seconds %= 60;
        long hours = minutes / 60;
        minutes %= 60;
        String retval = "";
        if (milliseconds > 0) {
            retval = " " + milliseconds + " milliseconds" + retval;
        }
        if (seconds > 0) {
            retval = " " + seconds + " seconds" + retval;
        }
        if (minutes > 0) {
            retval = " " + minutes + " minutes" + retval;
        }
        if (hours > 0) {
            retval = " " + hours + " hours" + retval;
        }
        return retval;
    }
}
