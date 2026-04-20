/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class PublicCloudTargetImporter<
    BP extends DatabasePersistable & BlobTarget< BP >,
    T extends ReplicationTarget< T >,
    ID extends ImportDirective< ID > & DatabasePersistable > 
    extends BaseImporter< BP, ID, TargetFailureType, ReplicationTargetImportHandler >
{
    public PublicCloudTargetImporter(
            final Class< BP > blobPersistenceTargetType,
            final Class< T > targetType,
            final UUID targetId,
            final Class< ? extends ImportDirectiveService< ID > > importDirectiveServiceType,
            final ReplicationTargetImportHandler importHandler,
            final BeansServiceManager serviceManager )
    {
        super( targetType.getSimpleName() + " " + targetId,
                blobPersistenceTargetType,
                Require.beanPropertyEquals( BlobTarget.TARGET_ID, targetId ),
                TargetFailureType.IMPORT_FAILED,
                TargetFailureType.IMPORT_INCOMPLETE,
                importDirectiveServiceType,
                targetId,
                importHandler,
                serviceManager,
                null );
        
        m_targetType = targetType;
        m_targetId = targetId;
        Validations.verifyNotNull( "Target type", m_targetType );
        Validations.verifyNotNull( "Target id", m_targetId );
    }


    @Override
    protected BlobStoreTaskState finalizeImport( final BeansRetrieverManager brm )
    {
        return m_importHandler.finalizeImport();
    }


    @Override
    protected TargetFailureType verifyPriorToImport( final S3ObjectsOnMedia objectsOnMedia )
    {
        return null;
    }


    @Override
    protected UUID importBucketBegun( final Bucket bucket, final BeansServiceManager brm )
    {
        m_dataPolicyId = bucket.getDataPolicyId();
        
        final WhereClause findRuleFilter = Require.all( 
                Require.beanPropertyEquals( DataReplicationRule.TARGET_ID, m_targetId ),
                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, m_dataPolicyId ) );
        final AzureDataReplicationRule azureRule = 
                brm.getRetriever( AzureDataReplicationRule.class ).retrieve( findRuleFilter );
        final S3DataReplicationRule s3Rule = 
                brm.getRetriever( S3DataReplicationRule.class ).retrieve( findRuleFilter );

        if ( null != azureRule )
        {
            m_ruleId = azureRule.getId();
            m_degradedBlobRuleProperty = DegradedBlob.AZURE_REPLICATION_RULE_ID;
        }
        else if ( null != s3Rule )
        {
            m_ruleId = s3Rule.getId();
            m_degradedBlobRuleProperty = DegradedBlob.S3_REPLICATION_RULE_ID;
        }
        else
        {
            throw new UnsupportedOperationException( 
                    "Could not import bucket " + bucket.getName() 
                    + " since no data replication rule could be found for data policy " 
                    + getDataPolicyName( bucket.getDataPolicyId() ) + " that targets " 
                    + m_targetType.getSimpleName() + " " + m_targetId + "." );
        }
        
        return m_ruleId;
    }


    @Override
    protected void performFailureCleanup( final BeansServiceManager brm )
    {
    	//no additional cleanup necessary for this importer type
    }
    
    
    @Override
    protected void populateBlobPersistence(
            final UUID bucketId,
            final Map< UUID, Integer > orderIndexes,
            final BP bp ) throws Exception
    {
        bp.setTargetId( m_targetId );
    }


    @Override
    protected void deleteDegradedBlobs( final DegradedBlobService service, final Set< UUID > blobIds )
    {
        service.deleteForReplicationRule( 
                m_degradedBlobRuleProperty, 
                m_ruleId,
                m_blobPersistenceTargetType,
                m_targetId,
                blobIds );
    }


    private UUID m_ruleId;
    private String m_degradedBlobRuleProperty;
    private UUID m_dataPolicyId;
    private final Class< T > m_targetType;
    private final UUID m_targetId;
}
