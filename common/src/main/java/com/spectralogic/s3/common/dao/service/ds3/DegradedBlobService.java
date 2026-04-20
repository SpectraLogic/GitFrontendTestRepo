/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface DegradedBlobService extends BeansRetriever< DegradedBlob >, BeanCreator< DegradedBlob >, BeanDeleter
{
    void migrate(
            final String degradedBlobRuleProperty,
            final UUID bucketId,
            final UUID destRuleId,
            final UUID srcRuleId );
    
    
    void deleteAllForPersistenceRule( final UUID dataPersistenceRuleId );
    
    
    void deleteForPersistenceRule( 
            final UUID dataPersistenceRuleId, 
            final Class< ? extends DatabasePersistable > persistenceTargetType,
            final String persistenceTargetPropertyName,
            final UUID persistenceTargetId,
            final Set< UUID > blobIds );
    
    
    void deleteForReplicationRule( 
            final String degradedBlobPropertyName,
            final UUID ruleId, 
            final Class< ? extends DatabasePersistable > blobTargetType,
            final UUID targetId,
            final Set< UUID > blobIds );
    
    
    void deleteAllForReplicationRule(
            final String replicationRulePropertyName, 
            final UUID dataReplicationRuleId );
    
    
    < T extends PersistenceTarget< T >, BT extends DatabasePersistable > void blobsLostLocally( 
            final Class< T > persistenceTargetClass,
            final Class< BT > blobPersistenceTargetClass,
            final UUID persistenceTargetId,
            String error,
            final Set< UUID > blobIds );
    
    
    < T extends DatabasePersistable & BlobTarget< T >, R extends Identifiable & DataReplicationRule< R > > 
    void blobsLostOnTarget( 
            final Class< T > blobTargetType,
            final Class< R > replicationRuleType,
            final String degradedBlobReplicationRulePropertyName,
            final UUID targetId,
            String error,
            final Set< UUID > blobIds );
}
