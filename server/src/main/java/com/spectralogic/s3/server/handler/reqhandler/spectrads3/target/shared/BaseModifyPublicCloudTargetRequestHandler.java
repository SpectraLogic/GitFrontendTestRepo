/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.GenericFailure;

public abstract class BaseModifyPublicCloudTargetRequestHandler
    < T extends PublicCloudReplicationTarget< T > & DatabasePersistable, 
      R extends DataReplicationRule< R > & DatabasePersistable,
      BN extends PublicCloudTargetBucketName< BN > & DatabasePersistable >
    extends BaseModifyBeanRequestHandler< T >
{
    protected BaseModifyPublicCloudTargetRequestHandler( 
            final Class< T > targetType,
            final Class< R > ruleType,
            final Class< BN > bucketNameType,
            final RestDomainType restDomainType )
    {
        super( targetType,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                restDomainType );
    
        m_ruleType = ruleType;
        m_bucketNameType = bucketNameType;
        registerOptionalBeanProperties(
                NameObservable.NAME,
                PublicCloudReplicationTarget.HTTPS,
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.QUIESCED,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }
    
    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final T target,
            final Set< String > modifiedProperties )
    {
        final BeansRetriever< T > retriever =  params.getServiceManager().getRetriever( m_daoType );
        if ( modifiedProperties.contains( ReplicationTarget.QUIESCED ) ) 
        { 
            BaseModifyAllTargetsRequestHandler.validateQuiescedValueChange( retriever, target );
        }
        if ( modifiedProperties.contains( PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS ) )
        {
            validateAutoVerificationFrequency( target );
        }
        if ( modifiedProperties.contains( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX )
                || modifiedProperties.contains( PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX ) )
        {
            validateCloudBucketMappingChange( params, target );
        }
    }
    
    
    final static void validateAutoVerificationFrequency( final PublicCloudReplicationTarget< ? > target )
    {
        if ( null != target.getAutoVerifyFrequencyInDays()
                && 1 > target.getAutoVerifyFrequencyInDays().intValue() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "If specified, the auto verification frequency must be at least 1 day." );
        }
    }
    
    
    private void validateCloudBucketMappingChange( final CommandExecutionParams params, final T target )
    {
        if ( isImplicitCloudBucketMapping( params.getServiceManager(), target.getId() ) )
        {
            throw new S3RestException( 
                    GenericFailure.CONFLICT,
                    "Performing the requested modification would change implicit mappings of local buckets " 
                    + "to cloud buckets that would break the correct real-world mapping." );
        }
    }
    
    
    private boolean isImplicitCloudBucketMapping( 
            final BeansRetrieverManager brm,
            final UUID targetId )
    {
        return ( 0 < brm.getRetriever( Bucket.class ).getCount( Require.all( 
                Require.not( Require.exists( 
                        m_bucketNameType,
                        PublicCloudTargetBucketName.BUCKET_ID, 
                        Require.beanPropertyEquals( PublicCloudTargetBucketName.TARGET_ID, targetId ) ) ),
                Require.exists( 
                        Bucket.DATA_POLICY_ID, 
                        Require.exists(
                                m_ruleType, 
                                DataPlacement.DATA_POLICY_ID,
                                Require.beanPropertyEquals( 
                                        DataReplicationRule.TARGET_ID,
                                        targetId ) ) ) ) ) );
    }
    
    
    private final Class< R > m_ruleType;
    private final Class< BN > m_bucketNameType;
}
