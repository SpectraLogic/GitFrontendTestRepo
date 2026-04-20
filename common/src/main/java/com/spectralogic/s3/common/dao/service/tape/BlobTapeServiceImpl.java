/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.CollectionFactory;

final class BlobTapeServiceImpl extends BaseService< BlobTape > implements BlobTapeService
{
    BlobTapeServiceImpl()
    {
        super( BlobTape.class );
    }
    
    
    @Override
    public int getNextOrderIndex( final UUID tapeId )
    {
        return (int)getDataManager().getMax( 
                BlobTape.class, 
                BlobTape.ORDER_INDEX,
                Require.beanPropertyEquals( BlobTape.TAPE_ID, tapeId ) ) + 1;
    }
    

    @Override
    public void obsoleteBlobTapes(  final Set< BlobTape > blobTargets, final UUID obsoletion )
    {
        final Set< ObsoleteBlobTape > beans = new HashSet<>();
        for ( final BlobTape blobTarget : blobTargets )
        {
            final ObsoleteBlobTape bean = BeanFactory.newBean( ObsoleteBlobTape.class );
            BeanCopier.copy( bean, blobTarget );
            bean.setObsoletionId( obsoletion );
            beans.add( bean );
        }

        getServiceManager().getService( ObsoleteBlobTapeService.class ).create( beans );
    }
    
    
    @Override
    public void reclaimTape( final String cause, final UUID tapeId )
    {
        final Tape tape = getServiceManager().getRetriever( Tape.class ).attain( tapeId );
        blobsLost(
                null,
                tapeId,
                BeanUtils.< UUID >extractPropertyValues(
                        getServiceManager().getRetriever( BlobTape.class ).retrieveAll(
                                BlobTape.TAPE_ID, tapeId ).toSet(), BlobObservable.BLOB_ID ) );
        
        if ( null != tape.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain =
                    new TapeRM( tape, getServiceManager() ).getStorageDomainMember().getStorageDomain().unwrap();
            if ( storageDomain.isSecureMediaAllocation() )
            {
                LOG.info( "Tape has been allocated to storage domain " + storageDomain.getId() 
                        + ", which is configured for secure media allocation.  " 
                        + "Thus, the tape must continue to be assigned to its current assignment." );
                return;
            }
        }
        
        getDataManager().updateBean( 
                CollectionFactory.toSet(
                        PersistenceTarget.BUCKET_ID,
                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                        PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN ),
                BeanFactory.newBean( Tape.class ).setId( tapeId ) );
    }
    
    
    @Override
	public void delete( final Set< UUID > ids )
	{
    	super.delete( ids );
		final Set< UUID > tapeIds = BeanUtils.extractPropertyValues( retrieveAll( ids ).toSet() , BlobTape.TAPE_ID );
		for ( final UUID tapeId : tapeIds )
		{
			getServiceManager().getService( TapeService.class ).updateAssignment( tapeId );
		}
	}
    
    
    @Override
    public void blobsLost( String error, final UUID tapeId, final Set< UUID > blobIds )
    {
        getServiceManager().getService( DegradedBlobService.class ).blobsLostLocally( 
                Tape.class, BlobTape.class, tapeId, error, blobIds );
    }


    public void blobsSuspect( final String error, final Set< BlobTape > blobTargets )
    {
        LOG.warn( blobTargets.size() + " blobs are suspected to be degraded since " + error + "." );
        
        final Set< SuspectBlobTape > beans = new HashSet<>();
        for ( final BlobTape blobTarget : blobTargets )
        {
            final SuspectBlobTape bean = BeanFactory.newBean( SuspectBlobTape.class );
            BeanCopier.copy( bean, blobTarget );
            beans.add( bean );
        }
        
        getServiceManager().getService( SuspectBlobTapeService.class ).create( beans );
    }
    
    
    @Override
    public void reclaimForDeletedPersistenceRule( final UUID dataPolicyId, final UUID storageDomainId )
    {
        getDataManager().updateBeans( 
                CollectionFactory.toSet( PersistenceTarget.BUCKET_ID ),
                BeanFactory.newBean( Tape.class ), 
                Require.all( 
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ) ),
                        Require.exists( 
                                PersistenceTarget.BUCKET_ID,
                                Require.beanPropertyEquals( Bucket.DATA_POLICY_ID, dataPolicyId ) ) ) );
        getDataManager().deleteBeans( BlobTape.class, Require.all( 
                Require.exists(
                        BlobTape.TAPE_ID, 
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID,
                                        storageDomainId ) ) ),
                Require.exists( 
                        BlobObservable.BLOB_ID, 
                        Require.exists( 
                                Blob.OBJECT_ID,
                                Require.exists(
                                        S3Object.BUCKET_ID,
                                        Require.beanPropertyEquals( 
                                                Bucket.DATA_POLICY_ID, dataPolicyId ) ) ) ) ) );
    }
}
