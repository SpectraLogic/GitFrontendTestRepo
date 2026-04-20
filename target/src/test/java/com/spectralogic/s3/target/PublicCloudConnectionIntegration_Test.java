/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.target.testrunners.*;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;

import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.DataAlwaysOnlinePublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.DataOfflineablePublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.azuretarget.DefaultAzureConnectionFactory;
import com.spectralogic.s3.target.s3target.DefaultS3ConnectionFactory;

import com.spectralogic.util.find.PackageContentFinder;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.predicate.UnaryPredicate;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag( "public-cloud-integration" )
public final class PublicCloudConnectionIntegration_Test
{
    @Test
    public void testEveryPublicCloudConnectionType()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported()  )
        {
            return;
        }

        final Set< Class< ? > > testedTypes = new HashSet<>();
        testedTypes.add( AzureConnection.class );
        testedTypes.add( S3Connection.class );

        final PackageContentFinder finder = new PackageContentFinder(
                S3Connection.class.getPackage().getName(), S3Connection.class, null );
        final Set< Class< ? > > types = finder.getClasses( new UnaryPredicate< Class<?> >()
        {
            public boolean test( final Class< ? > element )
            {
                return PublicCloudConnection.class.isAssignableFrom( element );
            }
        } );
        types.remove( PublicCloudConnection.class );
        types.remove( DataAlwaysOnlinePublicCloudConnection.class );
        types.remove( DataOfflineablePublicCloudConnection.class );
        if ( !testedTypes.containsAll( types ) )
        {
            fail( "Not all types " + types + " are tested " + testedTypes + "." );
        }


        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final Set< Runnable > runnables = new HashSet<>();

        // Execute Azure test runners
        Set<Runnable> azureRunners = createAzureTestRunners(dbSupport, new AzureConnectionCreator(dbSupport));
        invokeAndWait(
                PublicCloudConnectionIntegration_Test.class.getSimpleName() + " - Azure",
                azureRunners.size(),
                azureRunners,
                800
        );

        // Execute S3 test runners
        Set<Runnable> s3Runners = createS3TestRunners(dbSupport, new S3ConnectionCreator(dbSupport));
        invokeAndWait(
                PublicCloudConnectionIntegration_Test.class.getSimpleName() + " - S3",
                s3Runners.size(),
                s3Runners,
                800
        );

        // This uses real AWS endpoint and used to test null endpoints.
        //Null endpoints does not work with Localstack. Uncomment for testing.
        /*Set<Runnable> s3RunnersAWS = createS3TestRunners(dbSupport, new S3ConnectionCreator(dbSupport, true));
        invokeAndWait(
                PublicCloudConnectionIntegration_Test.class.getSimpleName() + " - S3",
                s3RunnersAWS.size(),
                s3RunnersAWS,
                800
        );*/


        Set<Runnable> nativeRunners = createNativeModeTestRunners(dbSupport, new S3NativeConnectionCreator(dbSupport));
        invokeAndWait(
                PublicCloudConnectionIntegration_Test.class.getSimpleName() + " - Native",
                nativeRunners.size(),
                nativeRunners,
                800
        );

        // This uses real AWS endpoint and used to test null endpoints.
        //Null endpoints does not work with Localstack. Uncomment for testing.
       /* Set<Runnable> nativeRunnersAWS = createNativeModeTestRunners(dbSupport, new S3NativeConnectionCreator(dbSupport, true));
        invokeAndWait(
                PublicCloudConnectionIntegration_Test.class.getSimpleName() + " - Native",
                nativeRunnersAWS.size(),
                nativeRunnersAWS,
                800
        );*/


        String msg = "Test results:";
        final List< String > keys = new ArrayList<>( BaseTestRunner.RESULTS.keySet() );
        Collections.sort( keys );
        for ( final String key : keys )
        {
            msg += Platform.NEWLINE + BaseTestRunner.RESULTS.get( key );
        }
        LOG.info( msg );
    }



    private Set< Runnable > createCommonTestRunners(
            final DatabaseSupport dbSupport,
            final ConnectionCreator< ? > cc )
    {
        final Set< Runnable > retval = new HashSet<>();
        retval.add( new CreateBucketTestRunner( dbSupport, cc ) );
        retval.add( new DeleteOldBucketsTestRunner( dbSupport, cc ) );
        retval.add( new BucketOwnershipTestRunner( dbSupport, cc ) );
        retval.add( new GetBucketContractTestRunner( dbSupport, cc ) );
        retval.add( new BasicFunctionalTestRunner( dbSupport, cc ) );
        retval.add( new DataIntegrityTestRunner( dbSupport, cc ) );
        retval.add( new DataIntegrityUponDiscoveryTestRunner( dbSupport, cc ) );
        retval.add( new DiscoveryFailureTestRunner( dbSupport, cc ) );
        retval.add( new LargeBlobTestRunner( dbSupport, cc ) );
        retval.add( new ImportManyObjectsTestRunner( dbSupport, cc ) );
        retval.add( new BlobsAndBlobPartsTestRunner( dbSupport, cc ) );
        return retval;

    }

    private Set< Runnable > createAzureTestRunners(
            final DatabaseSupport dbSupport,
            final ConnectionCreator< ? > cc )
    {
        final Set< Runnable > retval = new HashSet<>(createCommonTestRunners(
                dbSupport,
                cc));
        retval.add( new SuspectAzureBlobTestRunner( dbSupport, cc ) );
        return retval;
    }

    private Set< Runnable > createS3TestRunners(
            final DatabaseSupport dbSupport,
            final ConnectionCreator< ? > cc )
    {
        final Set< Runnable > retval = new HashSet<>(createCommonTestRunners(
                dbSupport,
                cc));
        retval.add( new SuspectS3BlobReadTestRunner( dbSupport, cc ) );
        return retval;
    }
    
    private Set< Runnable > createNativeModeTestRunners(
            final DatabaseSupport dbSupport,
            final ConnectionCreator< ? > cc )
    {
        final Set< Runnable > retval = new HashSet<>();
        retval.add( new SuspectS3NativeBlobReadTestRunner( dbSupport, cc ) );
        return retval;
    }


    private final static class AzureConnectionCreator implements ConnectionCreator< AzureTarget >
    {
        private AzureConnectionCreator( final DatabaseSupport dbSupport )
        {
            m_factory = new DefaultAzureConnectionFactory( dbSupport.getServiceManager() );

            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            m_target = mockDaoDriver.createAzureTarget( getClass().getSimpleName() );
            m_target.setAccountName( PublicCloudSupport.AZURE_ACCOUNT_NAME );
            m_target.setAccountKey( PublicCloudSupport.AZURE_ACCOUNT_KEY );
            mockDaoDriver.updateBean( m_target, AzureTarget.ACCOUNT_NAME, AzureTarget.ACCOUNT_KEY );
        }


        public AzureConnection create()
        {
            return m_factory.connect( m_target );
        }


        private final AzureTarget m_target;
        private final DefaultAzureConnectionFactory m_factory;
    } // end inner class def


    private final static class S3ConnectionCreator implements ConnectionCreator< S3Target >
    {
        private S3ConnectionCreator( final DatabaseSupport dbSupport )
        {
            m_factory = new DefaultS3ConnectionFactory( dbSupport.getServiceManager() );

            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

            m_target = mockDaoDriver.createS3TargetToAmazon( getClass().getSimpleName() );
            m_target.setAccessKey( PublicCloudSupport.S3_ACCESS_KEY );
            m_target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
            m_target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
            m_target.setRegion(S3Region.US_EAST_1);
            m_target.setHttps(false);
            mockDaoDriver.updateBean( m_target, S3Target.ACCESS_KEY, S3Target.SECRET_KEY, S3Target.DATA_PATH_END_POINT, S3Target.HTTPS );
        }

        private S3ConnectionCreator( final DatabaseSupport dbSupport, boolean aws ) {
            m_factory = new DefaultS3ConnectionFactory( dbSupport.getServiceManager() );

            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

            m_target = mockDaoDriver.createS3TargetToAmazon( getClass().getSimpleName() );
            m_target.setAccessKey("AKIA2MDRNXD62VYN3EO4");
            m_target.setSecretKey("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");
            m_target.setRegion(S3Region.US_WEST_1);
            m_target.setHttps(false);
            mockDaoDriver.updateBean( m_target, S3Target.ACCESS_KEY, S3Target.SECRET_KEY, S3Target.DATA_PATH_END_POINT, S3Target.HTTPS );
        }


        public S3Connection create()
        {
            return m_factory.connect( m_target );
        }


        private final S3Target m_target;
        public final DefaultS3ConnectionFactory m_factory;
    } // end inner class def
    
    
    private final static class S3NativeConnectionCreator implements ConnectionCreator< S3Target >
    {
        private S3NativeConnectionCreator( final DatabaseSupport dbSupport )
        {
            m_factory = new DefaultS3ConnectionFactory( dbSupport.getServiceManager() );

            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            m_target = mockDaoDriver.createS3TargetToAmazon( getClass().getSimpleName() );

            m_target.setAccessKey( PublicCloudSupport.S3_ACCESS_KEY );
            m_target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
            m_target.setNamingMode( CloudNamingMode.AWS_S3 );
            m_target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
            m_target.setHttps(false);
            m_target.setRegion(S3Region.US_EAST_1);
            mockDaoDriver.updateBean( m_target, S3Target.ACCESS_KEY, S3Target.SECRET_KEY, S3Target.DATA_PATH_END_POINT, S3Target.HTTPS );
        }

        /*
        This creates endpoint using AWS. Used for null endpoint testing.
         */
        private S3NativeConnectionCreator( final DatabaseSupport dbSupport, boolean aws ) {
            m_factory = new DefaultS3ConnectionFactory( dbSupport.getServiceManager() );

            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            m_target = mockDaoDriver.createS3TargetToAmazon( getClass().getSimpleName() );

            m_target.setAccessKey("AKIA2MDRNXD62VYN3EO4");
            m_target.setSecretKey("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");
            m_target.setRegion(S3Region.US_WEST_1);

            m_target.setNamingMode( CloudNamingMode.AWS_S3 );

            m_target.setHttps(false);

            mockDaoDriver.updateBean( m_target, S3Target.ACCESS_KEY, S3Target.SECRET_KEY, S3Target.DATA_PATH_END_POINT, S3Target.HTTPS );
        }


        public S3Connection create()
        {
            return m_factory.connect( m_target );
        }


        public final S3Target m_target;
        private final DefaultS3ConnectionFactory m_factory;
    } // end inner class def





    private static void invokeAndWait(
            final String baseThreadName,
            final int wpSize,
            final Set< Runnable > runnables,
            final int secondsTimeout )
    {
        final WorkPool wp = WorkPoolFactory.createWorkPool( wpSize, baseThreadName );

        final Set< Future< ? > > futures = new HashSet<>();
        final Map< Future< ? >, String > runnableNames = new HashMap<>();
        for ( final Runnable r : runnables )
        {
            final Future< ? > f = wp.submit( r );
            futures.add( f );
            runnableNames.put( f, r.getClass().getSimpleName() );
        }

        String futureName = "";
        try
        {
            for ( final Future< ? > f : futures )
            {
                futureName = runnableNames.get( f );
                f.get( secondsTimeout, TimeUnit.SECONDS );
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Runnable '" + futureName + "' failed.", ex );
        }
        finally
        {
            wp.shutdownNow();
        }
    }


    public static byte[] getTestRequestPayload()
    {
        try
        {
            return "This is the request payload that we are verifying the checksum for.".getBytes( "UTF-8" );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    public static String getBucketName()
    {
        return "b" + NEXT_BUCKET_NUM.getAndIncrement();
    }

    private final static AtomicInteger NEXT_BUCKET_NUM = new AtomicInteger( 1 );
    private final static Logger LOG = Logger.getLogger( PublicCloudConnectionIntegration_Test.class );

}
