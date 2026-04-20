/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class DataPolicyServiceImpl_Test 
{
    @Test
    public void testIsReplicatedReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp1.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp2.getId(), DataReplicationRuleType.RETIRED, target.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp3.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final DataPolicyService service = 
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertFalse(service.isReplicated( dp1.getId() ), "Shoulda reported only replicated data policies as being replicated.");
        assertFalse(service.isReplicated( dp2.getId() ), "Shoulda reported only replicated data policies as being replicated.");
        assertTrue(service.isReplicated( dp3.getId() ), "Shoulda reported only replicated data policies as being replicated.");
    }
    
    
    @Test
    public void testhasAnyStorageDomainsUsingObjectNamesOnLtfsReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" )
                .setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME );
        mockDaoDriver.updateBean( 
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ), 
                StorageDomain.LTFS_FILE_NAMING );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp3.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp3.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyService service = 
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertFalse(service.hasAnyStorageDomainsUsingObjectNamesOnLtfs( dp1.getId() ), "Shoulda reported no storage domains using object names.");
        assertTrue(service.hasAnyStorageDomainsUsingObjectNamesOnLtfs( dp2.getId() ), "Shoulda reported storage domains using object names.");
        assertTrue(service.hasAnyStorageDomainsUsingObjectNamesOnLtfs( dp3.getId() ), "Shoulda reported storage domains using object names.");
    }
    
    
    @Test
    public void testareStorageDomainsWithObjectNamingAllowedReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" )
                .setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" )
                .setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME );
        mockDaoDriver.updateBean( 
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ), 
                StorageDomain.LTFS_FILE_NAMING );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp3.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp3.getId(), "bucket3" );
        
        mockDaoDriver.createObject( bucket1.getId(), "fileasdf" );
        mockDaoDriver.createObject( bucket2.getId(), "file:o1" );
        //Bucket3 has a ltfs object naming storage domain linked to it, so putting an object with a colon
        //in the filename would fail if we were doing it through the request handlers. However, we are
        //at a lower level, and thus bypassing the check. This allows us to prove in a later assert that
        //we do in fact return true for areStorageDomainsWithObjectNamingAllowed() without ever checking
        //the object names IF we already have an object-naming storage domain. This isn't strictly
        //necessary behavior, but it could be much more efficient in cases in cases where we have millions
        //of objects and we don't want to check their names for colons when we know we already have ltfs
        //object-named storage domains.
        mockDaoDriver.createObject( bucket3.getId(), "file:o1" );
        
        final DataPolicyService service = 
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertTrue(service.areStorageDomainsWithObjectNamingAllowed( dp1 ), "Shoulda reported object naming allowed.");
        assertTrue(service.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda reported object naming allowed.");
        assertTrue(service.areStorageDomainsWithObjectNamingAllowed( dp3 ), "Shoulda reported object naming allowed.");
    }
}
