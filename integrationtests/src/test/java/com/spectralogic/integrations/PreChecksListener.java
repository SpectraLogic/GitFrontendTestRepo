package com.spectralogic.integrations;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.io.IOException;

import static com.spectralogic.integrations.Ds3ApiHelpers.getTapesReady;


public class PreChecksListener implements TestExecutionListener {
    @Override
    public void testPlanExecutionStarted(final TestPlan testPlan) {
        // Your pre-requisite code goes here. This will run only once
        // before any tests start executing.
        System.out.println("Running one-time prerequisite for all tests.");
        Ds3Client client = TestUtils.setTestParams();
        try {
            getTapesReady(client);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }





}
