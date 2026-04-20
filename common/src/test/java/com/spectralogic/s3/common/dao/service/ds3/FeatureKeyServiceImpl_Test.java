/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class FeatureKeyServiceImpl_Test 
{
    @Test
    public void testCreateDeletesExistingKeyIfOneExists()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createFeatureKey( 
                FeatureKeyType.AWS_S3_CLOUD_OUT, Long.valueOf( 1 ), new Date() );
        mockDaoDriver.createFeatureKey( 
                FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT, Long.valueOf( 1 ), new Date() );
        
        create( dbSupport, 
                BeanFactory.newBean( FeatureKey.class ).setKey( FeatureKeyType.AWS_S3_CLOUD_OUT ) );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(FeatureKey.class).getCount(
                FeatureKey.EXPIRATION_DATE, null), "Shoulda been 1 key without a date.");

        create( dbSupport, 
                BeanFactory.newBean( FeatureKey.class ).setKey( FeatureKeyType.AWS_S3_CLOUD_OUT ) );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(FeatureKey.class).getCount(
                FeatureKey.EXPIRATION_DATE, null), "Shoulda been 1 key without a date.");

        create( dbSupport, 
                BeanFactory.newBean( FeatureKey.class ).setKey( FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT ) );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(FeatureKey.class).getCount(
                FeatureKey.EXPIRATION_DATE, null), "Shoulda been 2 keys without a date.");
    }
    
    
    private void create( final DatabaseSupport dbSupport, final FeatureKey fk )
    {
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( FeatureKeyService.class ).create( fk );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
}
