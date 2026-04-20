package com.spectralogic.s3.dataplanner.backend.tape;

import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;


import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TapeDriveResourceWrapper_Test  {

    @Test
    public void testCallbackFiresAfterAndOnlyAfterDelay() {
        final AtomicInteger callbackFires = new AtomicInteger(0);
        new TapeDriveResourceWrapper(
                InterfaceProxyFactory.getProxy(TapeDriveResource.class, null),
                () -> {callbackFires.incrementAndGet(); return true;},
                200L);
        assertEquals(
                0,
                callbackFires.get(),
                "Callback should not have fired yet" );
        TestUtil.sleep(300);
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should have fired by now" );
        TestUtil.sleep(300);
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should not have fired again");
    }

    @Test
    public void testCallbackResetsAfterRpcCall() {
        final AtomicInteger callbackFires = new AtomicInteger(0);
        final TapeDriveResource tapeDriveResource = new TapeDriveResourceWrapper(
                InterfaceProxyFactory.getProxy(TapeDriveResource.class, null),
                () -> {callbackFires.incrementAndGet(); return true;},
                200L);
        assertEquals(
                0,
                callbackFires.get(),
                "Callback should not have fired yet");
        for (int i = 0; i < 5; i++) {
            TestUtil.sleep(100);
            tapeDriveResource.ping();
        }
        assertEquals(
                0,
                callbackFires.get(),
                "Callback should not have fired yet" );
        TestUtil.sleep(300);
        assertEquals(
                 1,
                callbackFires.get(),
                "Callback should have fired by now");
        TestUtil.sleep(300);
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should not have fired again");
        tapeDriveResource.ping();
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should not have fired again yet " );
        TestUtil.sleep(300);
        assertEquals(
                2,
                callbackFires.get(),
                "Callback should have fired again by now" );
        TestUtil.sleep(300);
        assertEquals(
                2,
                callbackFires.get(),
                "Callback should not have fired again" );
    }


    @Test
    public void testRapidCallsInSuccession() {
        final long startTime = System.currentTimeMillis();
        final AtomicInteger callbackFires = new AtomicInteger(0);
        final TapeDriveResource tapeDriveResource = new TapeDriveResourceWrapper(
                InterfaceProxyFactory.getProxy(TapeDriveResource.class, null),
                () -> {callbackFires.incrementAndGet(); return true;},
                20L);
        assertEquals(
                 0,
                callbackFires.get(),
                "Callback should not have fired yet");
        for (int i = 0; i < 100; i++) {
            tapeDriveResource.ping();
            tapeDriveResource.inspect();
            tapeDriveResource.getLoadedTapeInformation();
        }
        TestUtil.sleep(100);
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should have fired once now");
        TestUtil.sleep(300);
        assertEquals(
                1,
                callbackFires.get(),
                "Callback should not have fired again");
        tapeDriveResource.ping();
        TestUtil.sleep(100);
        assertEquals(
                2,
                callbackFires.get(),
                        "Callback should have fired again by now" );
        TestUtil.sleep(300);
        assertEquals(
                2,
                callbackFires.get(),
                "Callback should not have fired again");
    }
}
