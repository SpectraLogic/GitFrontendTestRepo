/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator;

import java.util.concurrent.Future;

import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;



//TODO: These tests are failing on the build server, but work locally.
// For now they are disabled in build server builds using SKIP_RPC_TESTS env variable.
@Tag("rpc-integration")
public final class Simulator_Test
{
    @Test
    public void testGetStateManagerWaitsForInstanceAndReturnsNotNull()
    {
        final Simulator simulator = buildSimulator();
        final StateManagerGetter stateManagerGetter = new StateManagerGetter( simulator );
        final Future< ? > future = SystemWorkPool.getInstance().submit( stateManagerGetter );
        
        TestUtil.sleep( 10 );
        Assertions.assertFalse(future.isDone(),"Shoulda blocked trying to get state manager since simulator has not been started up yet." );
        
        SystemWorkPool.getInstance().submit( simulator );
        simulator.getStateManager( 20 );
        simulator.shutdown();
        
        int i = 1000;
        while ( --i > 0 && !future.isDone() )
        {
            TestUtil.sleep( 10 );
        }
        Assertions.assertTrue(future.isDone(),
                "Shoulda unblocked eventually since state manager "
            + "shoulda been constructed since simulator started." );
        Assertions.assertNotNull(stateManagerGetter.getStateManager(), "The returned state manager must not be null.");
    }
    
    @Test
    public void testGetStateManagerReturnsNotNullWhenInstanceAlreadyExists()
    {
        final Simulator simulator = buildSimulator();
        final Future< ? > future = SystemWorkPool.getInstance().submit( simulator );
        simulator.getStateManager( 20 );
        simulator.shutdown();
        int i = 1000;
        while (--i > 0 && !future.isDone())
        {
            TestUtil.sleep( 10 );
        }
        Assertions.assertTrue(future.isDone(), "The simulator shoulda shut down eventually.");
        Assertions.assertNotNull(simulator.getStateManager( 20 ), "The returned state manager must not be null.");
    }

    
    public Simulator buildSimulator()
    {
        return new Simulator();
    }
    

    private static final class StateManagerGetter implements Runnable
    {
        private StateManagerGetter( final Simulator simulator )
        {
            m_simulator = simulator;
        }
        
        
        public SimStateManager getStateManager()
        {
            return m_stateManager;
        }

        
        public void run()
        {
            m_stateManager = m_simulator.getStateManager( 20 );
        }
        
        
        private final Simulator m_simulator;
        private volatile SimStateManager m_stateManager;
    }// end inner class
}
