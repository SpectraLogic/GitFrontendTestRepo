/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.temp;

import java.util.HashSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class BlobS3TargetToVerifyServiceImpl_Test 
{
    @Test
    public void testVerifyDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BlobS3TargetToVerifyService service =
                dbSupport.getServiceManager().getService( BlobS3TargetToVerifyService.class );
        final SuspectBlobS3TargetService suspectService =
                dbSupport.getServiceManager().getService( SuspectBlobS3TargetService.class );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        final S3Object o5 = mockDaoDriver.createObject( null, "o5" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( null, "o6" );
        final Blob b6 = mockDaoDriver.getBlobFor( o6.getId() );
        
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        
        service.verifyBegun( target1.getId() );
        service.verifyCompleted( target1.getId() );
        assertEquals(0,  suspectService.getCount(), "Shoulda reported no suspect blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda reported no suspect blobs.");

        mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b4.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b5.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b5.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b6.getId() );
        
        service.verifyBegun( target1.getId() );
        final Object actual6 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b1.getId() ) );
        assertEquals(new HashSet<Object>(), actual6, "Shoulda reported no blobs eligible for deletion.");
        final Object actual5 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(new HashSet<Object>(), actual5, "Shoulda reported no blobs eligible for deletion.");
        final Object expected3 = CollectionFactory.toSet( o1.getId(), o2.getId() );
        assertEquals(expected3, service.blobsVerified( target1.getId(), CollectionFactory.toSet( o1.getId(), o2.getId() ) ), "Shoulda reported unknown blobs as eligible for deletion.");
        final Object expected2 = CollectionFactory.toSet( o2.getId() );
        assertEquals(expected2, service.blobsVerified( target1.getId(), CollectionFactory.toSet( b1.getId(), o2.getId() ) ), "Shoulda reported unknown blobs as eligible for deletion.");
        final Object actual4 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(new HashSet<Object>(), actual4, "Shoulda reported no blobs eligible for deletion.");
        service.verifyCompleted( target1.getId() );
        assertEquals(2,  suspectService.getCount(), "Shoulda reported suspect blobs for b4 and b5.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda reported suspect blobs.");

        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( SuspectBlobS3TargetService.class ).delete(
                    BeanUtils.toMap( suspectService.retrieveAll().toSet() ).keySet() );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        service.verifyBegun( target1.getId() );
        final Object actual3 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b1.getId() ) );
        assertEquals(new HashSet<Object>(), actual3, "Shoulda reported no blobs eligible for deletion.");
        final Object actual2 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(new HashSet<Object>(), actual2, "Shoulda reported no blobs eligible for deletion.");
        final Object expected1 = CollectionFactory.toSet( o1.getId(), o2.getId() );
        assertEquals(expected1, service.blobsVerified( target1.getId(), CollectionFactory.toSet( o1.getId(), o2.getId() ) ), "Shoulda reported unknown blobs as eligible for deletion.");
        final Object expected = CollectionFactory.toSet( o2.getId() );
        assertEquals(expected, service.blobsVerified( target1.getId(), CollectionFactory.toSet( b1.getId(), o2.getId() ) ), "Shoulda reported unknown blobs as eligible for deletion.");
        final Object actual1 = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b4.getId(), b5.getId() ) );
        assertEquals(new HashSet<Object>(), actual1, "Shoulda reported no blobs eligible for deletion.");
        service.verifyCompleted( target1.getId() );
        assertEquals(0,  suspectService.getCount(), "Shoulda reported no suspect blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda reported no suspect blobs.");

        service.verifyBegun( target1.getId() );
        final Object actual = service.blobsVerified( target1.getId(), CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(new HashSet<Object>(), actual, "Shoulda reported no blobs eligible for deletion.");
        service.verifyBegun( target1.getId() );
        service.verifyCompleted( target1.getId() );
        assertEquals(5,  suspectService.getCount(), "Shoulda reported suspect blobs for every blob.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda reported suspect blobs.");
    }
}
