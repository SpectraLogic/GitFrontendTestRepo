/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A {@link Blob} that is degraded and needs to be rebuilt.
 */
@UniqueIndexes(
{
    @Unique( { DegradedBlob.PERSISTENCE_RULE_ID, BlobObservable.BLOB_ID } ),
    @Unique( { DegradedBlob.DS3_REPLICATION_RULE_ID, BlobObservable.BLOB_ID } )
})
public interface DegradedBlob extends BlobObservable< DegradedBlob >, DatabasePersistable
{
    String BUCKET_ID = "bucketId";

    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Bucket.class )
    UUID getBucketId();
    
    DegradedBlob setBucketId( final UUID bucketId );
    
    
    String PERSISTENCE_RULE_ID = "persistenceRuleId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( DataPersistenceRule.class )
    UUID getPersistenceRuleId();
    
    DegradedBlob setPersistenceRuleId( final UUID value );
    
    
    String DS3_REPLICATION_RULE_ID = "ds3ReplicationRuleId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Ds3DataReplicationRule.class )
    UUID getDs3ReplicationRuleId();
    
    DegradedBlob setDs3ReplicationRuleId( final UUID value );
    
    
    String AZURE_REPLICATION_RULE_ID = "azureReplicationRuleId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( AzureDataReplicationRule.class )
    UUID getAzureReplicationRuleId();
    
    DegradedBlob setAzureReplicationRuleId( final UUID value );
    
    
    String S3_REPLICATION_RULE_ID = "s3ReplicationRuleId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( S3DataReplicationRule.class )
    UUID getS3ReplicationRuleId();
    
    DegradedBlob setS3ReplicationRuleId( final UUID value );
}
