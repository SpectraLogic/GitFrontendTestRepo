/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import com.spectralogic.s3.common.dao.domain.ds3.S3Region;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag( "public-cloud-integration" )
public final class DefaultS3ConnectionFactory_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultS3ConnectionFactory( null );
            }
        } );
    }
    
    @Test
    public void testConnectUpdatesTargetStateBasedOnWhetherOrNotConnectionWasSuccessful()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }

        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3ConnectionFactory factory = 
                new DefaultS3ConnectionFactory( dbSupport.getServiceManager() );
        final S3Target target = PublicCloudSupport.createS3Target( mockDaoDriver );
        target.setSecretKey( PublicCloudSupport.S3_BAD_SECRET_KEY );
        
        TestUtil.assertThrows( 
                null, 
                S3SdkFailure.valueOf( PublicCloudSupport.S3_403_AUTHENTICATION, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        factory.connect( target );
                    }
                } );

        mockDaoDriver.attainAndUpdate( target );
        assertEquals(TargetState.OFFLINE, target.getState(), "Shoulda set state to offline due to connection failure.");

        target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
        target.setHttps(false);
        target.setRegion(S3Region.US_EAST_1 );
        target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
        factory.connect( target ).shutdown();
        mockDaoDriver.attainAndUpdate( target );
        assertEquals(TargetState.ONLINE, target.getState(), "Shoulda set state to online due to connection success.");

        target.setSecretKey( PublicCloudSupport.S3_BAD_SECRET_KEY );
        TestUtil.assertThrows( 
                null, 
                S3SdkFailure.valueOf( PublicCloudSupport.S3_403_AUTHENTICATION, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        factory.connect( target );
                    }
                } );

        mockDaoDriver.attainAndUpdate( target );
        assertEquals(TargetState.OFFLINE, target.getState(), "Shoulda set state to offline due to connection failure.");
    }
}
