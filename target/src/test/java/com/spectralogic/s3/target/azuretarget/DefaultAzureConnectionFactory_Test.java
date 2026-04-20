/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
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
public final class DefaultAzureConnectionFactory_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultAzureConnectionFactory( null );
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
        final AzureConnectionFactory factory = 
                new DefaultAzureConnectionFactory( dbSupport.getServiceManager() );
        final AzureTarget target = PublicCloudSupport.createAzureTarget( mockDaoDriver );
        target.setAccountKey( PublicCloudSupport.AZURE_BAD_ACCOUNT_KEY );
        
        TestUtil.assertThrows( 
                null, 
                AzureSdkFailure.valueOf( PublicCloudSupport.AZURE_403_FAILURE, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        factory.connect( target );
                    }
                } );

        mockDaoDriver.attainAndUpdate( target );
        assertEquals(
                TargetState.OFFLINE,
                target.getState(),
                "Shoulda set state to offline due to connection failure."
                 );
        
        target.setAccountKey( PublicCloudSupport.AZURE_ACCOUNT_KEY );
        target.setHttps(false);
        factory.connect( target ).shutdown();
        mockDaoDriver.attainAndUpdate( target );
        assertEquals(
                TargetState.ONLINE,
                target.getState(),
                "Shoulda set state to online due to connection success."
                 );
        
        target.setAccountKey( PublicCloudSupport.AZURE_BAD_ACCOUNT_KEY );
        TestUtil.assertThrows( 
                null, 
                AzureSdkFailure.valueOf( PublicCloudSupport.AZURE_403_FAILURE, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        factory.connect( target );
                    }
                } );

        mockDaoDriver.attainAndUpdate( target );
        assertEquals(
                TargetState.OFFLINE,
                target.getState(),
                "Shoulda set state to offline due to connection failure."
                );
    }
}
