/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMemberState;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.ThrottledLog;

public final class PersistenceTargetImporter< 
    BP extends DatabasePersistable & BlobObservable< BP >,
    PT extends PersistenceTarget< PT > & DatabasePersistable, 
    ID extends ImportPersistenceTargetDirective< ID > & DatabasePersistable, 
    F > extends BaseImporter< BP, ID, F, PersistenceTargetImportHandler< F > >
{
    public PersistenceTargetImporter(
            final Class< BP > blobPersistenceTargetType,
            final Class< PT > persistenceTargetType,
            final UUID persistenceTargetId,
            final WhereClause candidateStorageDomainFilterForPersistenceTarget,
            final Class< ? extends ImportDirectiveService< ID > > importDirectiveServiceType,
            final String persistenceTargetPropertyName,
            final F importFailedCode,
            final F importIncompleteCode,
            final PersistenceTargetImportHandler< F > importHandler,
            final BeansServiceManager serviceManager,
            final BlobStore blobStore )
    {
        super( persistenceTargetType.getSimpleName() + " " + persistenceTargetId,
               blobPersistenceTargetType,
               Require.beanPropertyEquals( persistenceTargetPropertyName, persistenceTargetId ),
               importFailedCode,
               importIncompleteCode,
               importDirectiveServiceType,
               persistenceTargetId,
               importHandler,
               serviceManager,
               ( BlobTape.class.isAssignableFrom( blobPersistenceTargetType ) ) ? NO_BLOB_COUNT_LOG : null );
        m_persistenceTargetType = persistenceTargetType;
        m_persistenceTargetId = persistenceTargetId;
        m_candidateStorageDomainFilterForPersistenceTarget = candidateStorageDomainFilterForPersistenceTarget;
        m_persistenceTargetPropertyName = persistenceTargetPropertyName;
        m_blobStore = blobStore;

        Validations.verifyNotNull(
                "Persistence target type", m_persistenceTargetType );
        Validations.verifyNotNull(
                "Persistence target id", m_persistenceTargetId );
        Validations.verifyNotNull(
                "Candidate storage domain failure for persistence target",
                m_candidateStorageDomainFilterForPersistenceTarget );
        Validations.verifyNotNull( 
                "Import directive persistence target prop name",
                m_persistenceTargetPropertyName );
        Validations.verifyNotNull(
                "Blob store", m_blobStore );
    }
    
    
    @Override
    protected BlobStoreTaskState finalizeImport( final BeansRetrieverManager brm )
    {
        final BlobStoreTaskState retval = m_importHandler.finalizeImport(
                m_lastStorageDomainId,
                ( m_lastIsolated ) ? m_lastBucketId : null );
        if ( BlobStoreTaskState.COMPLETED == retval 
                && null != m_directive.getVerifyDataAfterImport() )
        {
            verifyImportedMedia( brm );
        }
        return retval;
    }
    
    
    private void verifyImportedMedia( final BeansRetrieverManager brm )
    {
        final Method methodGetState = 
                BeanUtils.getReader( m_persistenceTargetType, "state" );
        final PT pt = brm.getRetriever( m_persistenceTargetType ).attain(
                m_persistenceTargetId );
        try
        {
            if ( methodGetState.invoke( pt ).toString().equals( "NORMAL" ) )
            {
                m_blobStore.verify(
                        m_directive.getVerifyDataAfterImport(), 
                        m_persistenceTargetId );
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }


    @Override
    protected F verifyPriorToImport( final S3ObjectsOnMedia objectsOnMedia )
    {
        return m_importHandler.verify( m_directive, objectsOnMedia );
    }


    @Override
    protected UUID importBucketBegun( final Bucket bucket, final BeansServiceManager brm )
    {
        try
        {
            m_rule = determineDataPersistenceRule( bucket.getDataPolicyId(), brm );
        }
        catch ( final RuntimeException ex )
        {
            String msg = "Failed to determine the data persistence rule to use to import bucket '" 
                    + bucket.getName() + "'.";
            if ( null != m_directive.getDataPolicyId() 
                    && !m_directive.getDataPolicyId().equals( bucket.getDataPolicyId() ) )
            {
                msg += "  Note: While new buckets will be imported into data policy "
                        + getDataPolicyName( m_directive.getDataPolicyId() )
                        + " as was specified in the import request, bucket '" + bucket.getName()
                        + "' already exists and is using " 
                        + getDataPolicyName( bucket.getDataPolicyId() ) + ".";
                        
            }
            throw new RuntimeException( msg, ex );
        }
        
        m_lastIsolated =
                ( m_lastIsolated || ( DataIsolationLevel.STANDARD != m_rule.getIsolationLevel() ) );
        if ( null != m_lastStorageDomainId
                && !m_lastStorageDomainId.equals( m_rule.getStorageDomainId() ) )
        {
            throw new RuntimeException(
                    "Cannot import since different buckets residing on media must be " 
                    + "imported to different storage domains.  " 
                    + "Originally planned to import all contents into storage domain " 
                    + getStorageDomainName( m_lastStorageDomainId ) + ", but bucket " + bucket.getName()
                    + " needs to be imported into storage domain " 
                    + getStorageDomainName( m_rule.getStorageDomainId() ) + "." );
        }
        if ( null != m_lastBucketId && m_lastIsolated && !m_lastBucketId.equals( bucket.getId() ) )
        {
            throw new RuntimeException(
                    "Cannot import since there are multiple buckets on the " 
                    + m_persistenceTargetType.getSimpleName()
                    + ", but bucket-level data isolation is required by the data policy." );
        }
        
        m_lastStorageDomainId = m_rule.getStorageDomainId();
        m_importHandler.verifyCompatibleStorageDomain( m_lastStorageDomainId );
        m_lastBucketId = bucket.getId();
        LOG.info( "Will import bucket '" + bucket.getName() + "' into storage domain " 
                  + m_rule.getStorageDomainId() + " per data policy " + m_rule.getDataPolicyId() + "." );
        
        final PT mediaToImport = brm.getRetriever( m_persistenceTargetType ).attain( m_persistenceTargetId );
        final UUID storageDomainMemberId = brm.getService( StorageDomainService.class )
        		.selectAppropriateStorageDomainMember( mediaToImport, m_rule.getStorageDomainId() );
		brm.getUpdater( m_persistenceTargetType )
				.update( mediaToImport
						.setAssignedToStorageDomain( true )
						.setStorageDomainMemberId( storageDomainMemberId ),
						PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
						PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        return m_rule.getId();
    }
    
    
    @Override
	protected void performFailureCleanup( final BeansServiceManager brm )
    {
    	final PT mediaToImport = brm.getRetriever( m_persistenceTargetType ).attain( m_persistenceTargetId );
		brm.getUpdater( m_persistenceTargetType )
				.update( mediaToImport
						.setAssignedToStorageDomain( false )
						.setStorageDomainMemberId( null ),
						PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
						PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
    }
    
    
    private DataPersistenceRule determineDataPersistenceRule( 
            final UUID dataPolicyId,
            final BeansRetrieverManager brm )
    {
        final Set< DataPersistenceRule > candidates =
                getCandidateDataPersistenceRules( dataPolicyId, brm );
        if ( null != m_directive.getStorageDomainId() )
        {
            for ( final DataPersistenceRule candidate : candidates )
            {
                if ( m_directive.getStorageDomainId().equals( candidate.getStorageDomainId() ) )
                {
                    return candidate;
                }
            }
            
            final StorageDomain memberStorageDomain = 
                    brm.getRetriever( StorageDomain.class ).retrieve( Require.exists(
                            DataPersistenceRule.class,
                            DataPersistenceRule.STORAGE_DOMAIN_ID,
                            Require.all( 
                                    Require.beanPropertyEquals(
                                            DataPersistenceRule.STORAGE_DOMAIN_ID, 
                                            m_directive.getStorageDomainId() ),
                                    Require.beanPropertyEquals(
                                            DataPlacement.DATA_POLICY_ID,
                                            dataPolicyId ) ) ) );
            if ( null == memberStorageDomain )
            {
                throw new RuntimeException(
                        "There are no persistence rules in data policy " + getDataPolicyName( dataPolicyId )
                        + " that persist to storage domain " 
                        + getStorageDomainName( m_directive.getStorageDomainId() ) 
                        + "." );
            }
            throw new RuntimeException(
                    "While there is a persistence rule in data policy " + getDataPolicyName( dataPolicyId )
                    + " that would permit persistence to storage domain " 
                    + getStorageDomainName( m_directive.getStorageDomainId() ) 
                    + ", the way your storage domain and data policy are configured prohibit it.  "
                    + "This is most likely due to the storage domain memberships not including the media "
                    + "you're attempting to import." );
        }
        
        if ( 1 == candidates.size() )
        {
            return candidates.iterator().next();
        }
        if ( 1 < candidates.size() )
        {
            throw new RuntimeException(
                    "Cannot determine which persistence rule in data policy "
                    + getDataPolicyName( dataPolicyId ) + " to use.  " 
                    + candidates.size()
                    + " candidates exist: " + candidates 
                    + ".  The storage domain to import into must be specified." );
        }
        
        throw new RuntimeException(
                "Cannot determine which storage domain to import into.  Data policy "
                + getDataPolicyName( dataPolicyId )
                + " does not have any persistence rules pointing to any storage domains that can contain "
                + m_persistenceTargetType.getSimpleName() + " " + m_persistenceTargetId + " as a member." );
    }
    
    
    private Set< DataPersistenceRule > getCandidateDataPersistenceRules(
            final UUID dataPolicyId,
            final BeansRetrieverManager brm )
    {
        final Set< DataPersistenceRule > dataPolicyCandidateStorageDomains =
                brm.getRetriever( DataPersistenceRule.class ).retrieveAll( 
                        Require.beanPropertyEquals( 
                                DataPlacement.DATA_POLICY_ID, 
                                dataPolicyId ) ).toSet();
        return brm.getRetriever( DataPersistenceRule.class ).retrieveAll( Require.all( 
                Require.beanPropertyEqualsOneOf(
                        Identifiable.ID, 
                        BeanUtils.toMap( dataPolicyCandidateStorageDomains ).keySet() ),
                Require.exists( 
                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                        Require.exists(
                                StorageDomainMember.class,
                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                Require.all( 
                                        Require.beanPropertyEquals( 
                                                StorageDomainMember.STATE, 
                                                StorageDomainMemberState.NORMAL ),
                                        m_candidateStorageDomainFilterForPersistenceTarget ) ) ) ) ).toSet();
    }


    @Override
    protected void populateBlobPersistence(
            final UUID bucketId,
            final Map< UUID, Integer > orderIndexes,
            final BP bp ) throws Exception
    {
        final Method blobPersistenceTargetRecordPersistenceTargetIdWriter =
                BeanUtils.getWriter( 
                        m_blobPersistenceTargetType,
                        m_persistenceTargetPropertyName );
        blobPersistenceTargetRecordPersistenceTargetIdWriter.invoke( 
                bp, m_persistenceTargetId );
        final Method blobPersistenceTargetRecordOrderIndexWriter =
                BeanUtils.getWriter( m_blobPersistenceTargetType, "orderIndex" );
        if ( null != blobPersistenceTargetRecordOrderIndexWriter )
        {
            blobPersistenceTargetRecordOrderIndexWriter.invoke( bp, orderIndexes.get( bp.getBlobId() ) );
        }
        final Method blobPersistenceTargetBucketIdWriter =
                BeanUtils.getWriter( m_blobPersistenceTargetType, "bucketId" );
        if ( null != blobPersistenceTargetBucketIdWriter )
        {
            blobPersistenceTargetBucketIdWriter.invoke( bp, bucketId );
        }
    }


    @Override
    protected void deleteDegradedBlobs( final DegradedBlobService service, final Set< UUID > blobIds )
    {
        service.deleteForPersistenceRule( 
                m_rule.getId(),
                m_blobPersistenceTargetType,
                m_persistenceTargetPropertyName,
                m_persistenceTargetId,
                blobIds );
    }
    

    private DataPersistenceRule m_rule;
    private boolean m_lastIsolated;
    private UUID m_lastBucketId;
    private UUID m_lastStorageDomainId;
    
    private final Class< PT > m_persistenceTargetType;
    private final UUID m_persistenceTargetId;
    private final WhereClause m_candidateStorageDomainFilterForPersistenceTarget;
    private final String m_persistenceTargetPropertyName;
    private final BlobStore m_blobStore;
    
    private final static ThrottledLog NO_BLOB_COUNT_LOG = new ThrottledLog( LOG, 1000 );
}
