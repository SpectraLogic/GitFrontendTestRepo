package com.spectralogic.s3.common.dao.service.composite;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.DataMigration;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public interface IomService
{
    int MAX_BLOBS_IN_IOM_JOB = 100000;

    void cleanupOldMigrations( Consumer< DataMigration > preDeleteOperation );
    
    
    void markTapesForAutoCompaction();


    void markSingleTapeForAutoCompaction(final UUID tapeId);
    
    
    void markTapesFinishedAutoCompacting();
    
    
    //NOTE: method assumes any blobs passed here belong to the same data policy
    void removeDegradedBlobsThatHaveBeenHealed( Collection< UUID > blobIds );
    
    
    void handleDataPlacementRulesFinishedPendingInclusion();
    
    
    void handleStorageDomainMembersFinishedPendingExclusion();
    
    
    Set< DataMigration > getMigrationsInError();
    
    
    void deleteBlobRecordsThatHaveFinishedMigratingOrHealing();


    EnhancedIterable< Blob > getBlobsRequiringLocalIOMWork(
            final UUID bucketId,
            final DataPersistenceRule rule );
            
            
    < T extends DatabasePersistable & BlobTarget< ? >, S extends T > CloseableIterable< Set< Blob > >
			getBlobsRequiringIOMWorkOnTarget(
					final Class< T > blobTargetType,
					final Class< S > suspectBlobTargetType,
		            final UUID bucketId,
		            final UUID targetId,
                    final UUID dataPolicyId );
            
            
    public Set< DataPersistenceRule > getPermanentPersistenceRulesForBucket( final UUID bucketId );
    
    
    public boolean isIomEnabled();
    
    
    public boolean isNewJobCreationAllowed();
}
