/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class StorageDomainMemberServiceImpl_Test 
{
    @Test
    public void testEnsureWritePreferencesValidOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final StorageDomainMemberService service =
                dbSupport.getServiceManager().getService( StorageDomainMemberService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition tp3 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "sd4" );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp1.getId(), TapeType.LTO6, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp1.getId(), TapeType.LTO6, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp2.getId(), TapeType.LTO5, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd3.getId(), tp2.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), tp3.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp3.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    service.ensureWritePreferencesValid();
                }
            } );
    }
    
    
    @Test
    public void testEnsureWritePreferencesValidDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final StorageDomainMemberService service =
                transaction.getService( StorageDomainMemberService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null, null );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO5 );
        final TapePartition tp3 = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp1.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp1.getId(), TapeType.LTO6, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp1.getId(), TapeType.LTO7, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp2.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp2.getId(), TapeType.LTO6, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), tp2.getId(), TapeType.LTO7, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), tp3.getId(), TapeType.LTO5, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), tp3.getId(), TapeType.LTO6, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), tp3.getId(), TapeType.LTO7, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp1.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp1.getId(), TapeType.LTO6, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp1.getId(), TapeType.LTO7, WritePreferenceLevel.NORMAL );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp2.getId(), TapeType.LTO5, WritePreferenceLevel.NEVER_SELECT );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp2.getId(), TapeType.LTO6, WritePreferenceLevel.NEVER_SELECT );
        mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), tp2.getId(), TapeType.LTO7, WritePreferenceLevel.NEVER_SELECT );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp3.getId(), TapeType.LTO5, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp3.getId(), TapeType.LTO6, WritePreferenceLevel.LOW );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp3.getId(), TapeType.LTO7, WritePreferenceLevel.LOW );

        assertEquals(0,  service.getCount(Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        sd1.getId()),
                Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT))), "Every member of sd1 shoulda been writable initially.");
        assertEquals(3,  service.getCount(Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        sd2.getId()),
                Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT))), "Every tp1 and tp2 member of sd2 shoulda been writable initially.");

        service.ensureWritePreferencesValid();
        assertEquals(3,  service.getCount(Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        sd1.getId()),
                Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT))), "Shoulda marked members that aren't writable anymore as read only.");
        assertEquals(4,  service.getCount(Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        sd2.getId()),
                Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT))), "Shoulda marked members that aren't writable anymore as read only.");

        transaction.commitTransaction();
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Shoulda been failures reported - one for each member marked as read only.");
    }
}
