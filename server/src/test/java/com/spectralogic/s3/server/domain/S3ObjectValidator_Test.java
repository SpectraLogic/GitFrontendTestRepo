/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.Collections;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class S3ObjectValidator_Test
{
    @Test
    public void testVerifyNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                S3ObjectValidator.verify(
                        null,
                        UUID.randomUUID(), 
                        "valid" );
            }
        } );
    }
    

    @Test
    public void testVerifyNullBucketIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                S3ObjectValidator.verify(
                        InterfaceProxyFactory.getProxy( BeansServiceManager.class, null ),
                        (UUID)null,
                        "valid" );
            }
        } );
    }
    

    @Test
    public void testVerifyNullObjectNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                S3ObjectValidator.verify(
                        InterfaceProxyFactory.getProxy( BeansServiceManager.class, null ),
                        UUID.randomUUID(), 
                        null );
            }
        } );
    }
    

    @Test
    public void testVerifyZeroLengthObjectNameNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                S3ObjectValidator.verify(
                        InterfaceProxyFactory.getProxy( BeansServiceManager.class, null ),
                        UUID.randomUUID(), 
                        "" );
            }
        } );
    }
    

    @Test
    public void testVerifyValidObjectNameAllowed()
    {
        S3ObjectValidator.verify(
                InterfaceProxyFactory.getProxy( BeansServiceManager.class, null ),
                UUID.randomUUID(), 
                "valid" );
    }
    

    @Test
    public void testVerifyInvalidObjectNameAllowedIfBucketDoesNotTargetStorageDomainsInFullLtfsCompatibility()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dpolicy" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                sd1.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID ), 
                StorageDomain.LTFS_FILE_NAMING );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean(
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID ), 
                StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );

        assertEquals(false, dataPolicyService.hasAnyStorageDomainsUsingObjectNamesOnLtfs( dataPolicy.getId() ), "Shoulda said we have no storage domains with object naming.");
        assertEquals(true, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dataPolicy ), "Shoulda said storage domains with object naming are allowed.");
        S3ObjectValidator.verify(
                dbSupport.getServiceManager(),
                bucket.getId(),
                "in:valid" );
        assertEquals(true, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dataPolicy ), "Shoulda said storage domains with object naming still allowed since we didn't put object.");
    }
    

    @Test
    public void testVerifyInvalidObjectNameNotAllowedIfBucketDoesTargetStorageDomainsInFullLtfsCompatibility()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dpolicy" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                sd1.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID ), 
                StorageDomain.LTFS_FILE_NAMING );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean(
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID ), 
                StorageDomain.LTFS_FILE_NAMING );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        mockDaoDriver.updateBean(
                sd3.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ), 
                StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );

        assertEquals(true, dataPolicyService.hasAnyStorageDomainsUsingObjectNamesOnLtfs( dataPolicy.getId() ), "Shoulda said that we have storage domains using object names on ltfs.");
        TestUtil.assertThrows( "Shoulda thrown exception for putting a file with an invalid LTFS filename",
                GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                S3ObjectValidator.verify(
                        dbSupport.getServiceManager(),
                        bucket.getId(), "in/" + String.join( "", Collections.nCopies( 256, "a" ) ) + "/valid" );
            }
        } );
    }
}
