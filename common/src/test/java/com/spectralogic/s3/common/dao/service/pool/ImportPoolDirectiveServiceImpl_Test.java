/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ImportPoolDirectiveServiceImpl_Test 
{
    @Test
    public void testDeleteByBeanPropertyDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportPoolDirective( pool1.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.createImportPoolDirective( pool2.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportPoolDirectiveService service =
                dbSupport.getServiceManager().getService( ImportPoolDirectiveService.class );
        service.deleteByEntityToImport( pool1.getId() );

        assertEquals(1,  service.getCount(), "Shoulda whacked single directive.");
    }
    
    
    @Test
    public void testDeleteAllWhenPoolsExistThatAreImportPendingNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool.setState( PoolState.IMPORT_IN_PROGRESS ), Pool.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportPoolDirectiveService service =
                dbSupport.getServiceManager().getService( ImportPoolDirectiveService.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                    {
                        service.deleteAll();
                    }
                } );

            assertEquals(1,  service.getCount(), "Should notta whacked any directives.");
        }
    
    
    @Test
    public void testDeleteAllWhenPoolsExistThatAreImportInProgressNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool.setState( PoolState.IMPORT_IN_PROGRESS ), Pool.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportPoolDirectiveService service =
                dbSupport.getServiceManager().getService( ImportPoolDirectiveService.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    service.deleteAll();
                }
            } );

        assertEquals(1,  service.getCount(), "Should notta whacked any directives.");
    }
    
    
    @Test
    public void testDeleteAllWhenPoolsExistThatAreForeignAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool.setState( PoolState.FOREIGN ), Pool.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportPoolDirectiveService service =
                dbSupport.getServiceManager().getService( ImportPoolDirectiveService.class );
        service.deleteAll();

        assertEquals(0,  service.getCount(), "Shoulda whacked any directives.");
    }
}
