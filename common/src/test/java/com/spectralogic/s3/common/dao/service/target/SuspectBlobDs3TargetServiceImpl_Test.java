/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SuspectBlobDs3TargetServiceImpl_Test 
{

    @Test
    public void testSuspectRecordsBeingCreatedAndDeletedResultsInDataLossSystemFailureCreationAndDeletion()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final SuspectBlobDs3TargetService service = 
                dbSupport.getServiceManager().getService( SuspectBlobDs3TargetService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        
        final Tape tape = mockDaoDriver.createTape();
        final Pool pool = mockDaoDriver.createPool();
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        
        final SuspectBlobTape bTape = mockDaoDriver.makeSuspect( 
                mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() ) );
        final SuspectBlobPool bPool = mockDaoDriver.makeSuspect( 
                mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() ) );
        final SuspectBlobDs3Target bTarget = mockDaoDriver.makeSuspect( 
                mockDaoDriver.putBlobOnDs3Target( target.getId(), blob.getId() ) );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.delete( null );
            }
        } );
        deleteInsideTransaction( dbSupport.getServiceManager(), null );
        
        mockDaoDriver.delete( SuspectBlobPool.class, bPool );
        mockDaoDriver.delete( SuspectBlobTape.class, bTape );
        deleteInsideTransaction( dbSupport.getServiceManager(), null );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda whacked failure since no suspect records.");

        service.create( bTarget );
        assertEquals(SystemFailureType.CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION, mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType(), "Shoulda had a failure for the suspect record created.");

        deleteInsideTransaction( dbSupport.getServiceManager(), CollectionFactory.toSet( bTarget.getId() ) );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda whacked failure since no suspect records.");

        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( SuspectBlobDs3TargetService.class ).create(
                    CollectionFactory.toSet( bTarget ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(SystemFailureType.CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION, mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType(), "Shoulda had a failure for the suspect record created.");
    }
    
    
    private void deleteInsideTransaction( final BeansServiceManager serviceManager, final Set< UUID > ids )
    {
        final BeansServiceManager transaction = serviceManager.startTransaction();
        try
        {
            transaction.getService( SuspectBlobDs3TargetService.class ).delete( ids );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
}
