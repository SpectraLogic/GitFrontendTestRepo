/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class AzureTargetBucketNameService_Test 
{
    @Test
    public void testAttainTargetBucketNameReturnsCustomNameIfDefinedElseDefaultName()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        mockDaoDriver.updateBean( 
                target2.setCloudBucketPrefix( "pre" ), 
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        mockDaoDriver.updateBean( 
                target3.setCloudBucketPrefix( "pre" ), 
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        mockDaoDriver.updateBean( 
                target3.setCloudBucketSuffix( "post" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        mockDaoDriver.createAzureTargetBucketName( bucket2.getId(), target2.getId(), "custom" );
            
        final AzureTargetBucketNameService service =
                dbSupport.getServiceManager().getService( AzureTargetBucketNameService.class );
        assertEquals("b1", service.attainTargetBucketName( bucket1.getId(), target1.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb1", service.attainTargetBucketName( bucket1.getId(), target2.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb1post", service.attainTargetBucketName( bucket1.getId(), target3.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("b2", service.attainTargetBucketName( bucket2.getId(), target1.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("custom", service.attainTargetBucketName( bucket2.getId(), target2.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb2post", service.attainTargetBucketName( bucket2.getId(), target3.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
    }
    
    
    private void commonFixFailures( final MockDaoDriver mockDaoDriver, final String cloudBucketFix,
            Function< String, AzureTarget > function )
    {
        TestUtil.assertThrows( null, DaoException.class,
                () -> mockDaoDriver.updateBean( function.apply( "dashdot-.dashdot" ), cloudBucketFix ) );
        TestUtil.assertThrows( null, DaoException.class,
                () -> mockDaoDriver.updateBean( function.apply( "dotdash.-dotdash" ), cloudBucketFix ) );
        TestUtil.assertThrows( null, DaoException.class,
                () -> mockDaoDriver.updateBean( function.apply( "dot..dot" ), cloudBucketFix ) );
        TestUtil.assertThrows( null, DaoException.class,
                () -> mockDaoDriver.updateBean( function.apply( "under_score" ), cloudBucketFix ) );
        TestUtil.assertThrows( null, DaoException.class,
                () -> mockDaoDriver.updateBean( function.apply( "UPPER" ), cloudBucketFix ) );
        TestUtil.assertThrows( null, DaoException.class, () -> mockDaoDriver.updateBean(
                function.apply( "1234567890123456789012345678901234567890123456789012345678901234" ),
                cloudBucketFix ) );
    }
    
    
    private void commonFixSuccesses( final MockDaoDriver mockDaoDriver, final String cloudBucketFix,
            Function< String, AzureTarget > function )
    {
        mockDaoDriver.updateBean( function.apply( "" ), cloudBucketFix );
        mockDaoDriver.updateBean( function.apply( "a" ), cloudBucketFix );
        mockDaoDriver.updateBean( function.apply( "a." ), cloudBucketFix );
        mockDaoDriver.updateBean( function.apply( "123456789012345678901234567890123456789012345678901234567890123" ),
                cloudBucketFix );
        mockDaoDriver.updateBean( function.apply( "1.2.3.4" ), cloudBucketFix );
        // Invalid in Azure, valid in AWS
        mockDaoDriver.updateBean( function.apply( "dashdash--dashdash" ), cloudBucketFix );
    }
    
    
    @Test
    public void testBucketPrefixValidation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        
        commonFixFailures( mockDaoDriver, PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
                target::setCloudBucketPrefix );
        commonFixSuccesses( mockDaoDriver, PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
                target::setCloudBucketPrefix );
        
        TestUtil.assertThrows( null, DaoException.class, () -> mockDaoDriver
                .updateBean( target.setCloudBucketPrefix( ".dotlead" ),
                        PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX ) );
        TestUtil.assertThrows( null, DaoException.class, () -> mockDaoDriver
                .updateBean( target.setCloudBucketPrefix( "-dashlead" ),
                        PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX ) );
        
        mockDaoDriver.updateBean( target.setCloudBucketPrefix( "dottail." ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        mockDaoDriver.updateBean( target.setCloudBucketPrefix( "dashtail-" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
    }
    
    
    @Test
    public void testBucketSuffixValidation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        
        commonFixFailures( mockDaoDriver, PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
                target::setCloudBucketSuffix );
        commonFixSuccesses( mockDaoDriver, PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
                target::setCloudBucketSuffix );
        
        TestUtil.assertThrows( null, DaoException.class, () -> mockDaoDriver
                .updateBean( target.setCloudBucketSuffix( "dashtail-" ),
                        PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX ) );
        
        mockDaoDriver.updateBean( target.setCloudBucketSuffix( ".dotlead" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX );
        mockDaoDriver.updateBean( target.setCloudBucketSuffix( "-dashlead" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX );
    }
    
    @Test
    public void testAttainTargetBucketNameReturnsNameInAllLowerCase()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        mockDaoDriver.updateBean( 
                target2.setCloudBucketPrefix( "pre" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        mockDaoDriver.updateBean( 
                target3.setCloudBucketPrefix( "pre" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        mockDaoDriver.updateBean( 
                target3.setCloudBucketSuffix( "post" ),
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "B1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "B2" );
        
        mockDaoDriver.createAzureTargetBucketName( bucket2.getId(), target2.getId(), "custom" );
            
        final AzureTargetBucketNameService service =
                dbSupport.getServiceManager().getService( AzureTargetBucketNameService.class );
        assertEquals("b1", service.attainTargetBucketName( bucket1.getId(), target1.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb1", service.attainTargetBucketName( bucket1.getId(), target2.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb1post", service.attainTargetBucketName( bucket1.getId(), target3.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("b2", service.attainTargetBucketName( bucket2.getId(), target1.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("custom", service.attainTargetBucketName( bucket2.getId(), target2.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
        assertEquals("preb2post", service.attainTargetBucketName( bucket2.getId(), target3.getId() ), "Shoulda mapped local bucket name to correct public cloud bucket name.");
    }
}
