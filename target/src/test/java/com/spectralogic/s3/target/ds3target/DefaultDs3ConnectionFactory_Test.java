/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DefaultDs3ConnectionFactory_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultDs3ConnectionFactory( null );
            }
        } );
    }
    
    @Test
    public void testDiscoverNullTargetNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultDs3ConnectionFactory( new MockBeansServiceManager() )
                    .discover( null );
            }
        } );
    }
    
    @Test
    public void testDiscoverWhenCannotConnectFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "id" ).setAdminSecretKey( "key" );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                new DefaultDs3ConnectionFactory( dbSupport.getServiceManager() )
                    .discover( target );
            }
        } );
    }
    
    @Test
    public void testConnectNullTargetNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultDs3ConnectionFactory( new MockBeansServiceManager() )
                    .connect( null, null );
            }
        } );
    }
    
    @Test
    public void testConnectWhenCannotConnectFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Ds3Target target = mockDaoDriver.createDs3Target("target")
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "id" )
                .setAdminSecretKey( "key" );
        mockDaoDriver.updateBean(target,
                Ds3Target.DATA_PATH_END_POINT,
                Ds3Target.ADMIN_AUTH_ID,
                Ds3Target.ADMIN_SECRET_KEY);
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                new DefaultDs3ConnectionFactory( dbSupport.getServiceManager() )
                    .connect( null, target );
            }
        } );
        assertEquals(TargetState.OFFLINE, mockDaoDriver.attain(target).getState(), "Should have marked target offline");
    }

}
