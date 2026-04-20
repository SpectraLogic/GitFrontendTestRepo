package com.spectralogic.s3.target.testrunners;


import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public  abstract class BaseTestRunner implements Runnable{
    private final static Logger LOG = Logger.getLogger( BaseTestRunner.class );
    protected BaseTestRunner(final DatabaseSupport dbSupport, final ConnectionCreator< ? > cc )
    {
        m_dbSupport = dbSupport;
        m_connectionCreator = cc;
    }


    final public void run()
    {
        final PublicCloudConnection connection = m_connectionCreator.create();


        String result = null;
        final Duration duration = new Duration();
        final String description =
                getClass().getSimpleName() + "[" + connection.getClass().getSimpleName() + "]";
        try
        {
            Thread.currentThread().setName( description );
            runTest( m_dbSupport, connection );
            result = "PASSED: " + description + " [" + duration + "]";
            RESULTS.put( description, result );
            LOG.info( Platform.NEWLINE + result );
        }
        catch ( final Exception ex )
        {
            result = "FAILED: " + description + " [" + duration + "]";
            RESULTS.put( description, result );
            throw new RuntimeException( result, ex );
        }
    }


    protected abstract void runTest(
            final DatabaseSupport dbSupport,
            final PublicCloudConnection connection ) throws Exception;


    private final DatabaseSupport m_dbSupport;
    private final ConnectionCreator< ? > m_connectionCreator;


    public final static Map< String, String > RESULTS =
            Collections.synchronizedMap( new HashMap< String, String >() );
}
