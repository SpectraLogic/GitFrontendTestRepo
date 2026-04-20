/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.platform.replicationtarget.TargetInitializationUtil;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.GenericFailure;

public final class VerifySafeToCreatePutJobRequestHandler extends BaseRequestHandler
{
    public VerifySafeToCreatePutJobRequestHandler()
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.WRITE,
                AdministratorOverride.NO ), 
               new RestfulCanHandleRequestDeterminer(
                RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT,
                RestDomainType.BUCKET ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Bucket bucket = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Bucket.class ) );
        verifySafeToCreatePutJob( params.getServiceManager(), bucket.getDataPolicyId() );
        
        return BeanServlet.serviceGet( params, null );
    }

    
    static void verifySafeToCreatePutJob( final BeansRetrieverManager brm, final UUID dataPolicyId )
    {
        verifyEveryDs3TargetWeWillReplicateToIsAvailable( brm, dataPolicyId );
        verifyPublicCloudTargetLicensedIfInUse(
                brm, dataPolicyId, S3DataReplicationRule.class, FeatureKeyType.AWS_S3_CLOUD_OUT );
        verifyPublicCloudTargetLicensedIfInUse(
                brm, dataPolicyId, AzureDataReplicationRule.class, FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT );
    }
    
    
    private static void verifyEveryDs3TargetWeWillReplicateToIsAvailable( 
            final BeansRetrieverManager brm, 
            final UUID dataPolicyId )
    {
        final Set< Ds3Target > targets = brm.getRetriever( Ds3Target.class ).retrieveAll(
                TargetInitializationUtil.getInstance().getDs3TargetsToReplicateTo( brm, dataPolicyId ) )
                .toSet();
        for ( final Ds3Target target : targets )
        {
            if ( TargetState.ONLINE != target.getState() )
            {
                throw new S3RestException( 
                        GenericFailure.FORCE_FLAG_REQUIRED,
                        "DS3 target " + target.getId() + " (" + target.getName() 
                        + ") is in state " + target.getState() + "." );
            }
            if ( Quiesced.NO != target.getQuiesced() )
            {
                final String quiescedState = target.getQuiesced() == Quiesced.YES ? "quiesced" : "pending quiesced";
                throw new S3RestException( 
                        GenericFailure.FORCE_FLAG_REQUIRED,
                        "DS3 target " + target.getId() + " (" + target.getName() 
                        + ") is " + quiescedState + "." );
            }
        }
    }
    
    
    private static < R extends PublicCloudDataReplicationRule< R > & DatabasePersistable > 
    void verifyPublicCloudTargetLicensedIfInUse(
            final BeansRetrieverManager brm,
            final UUID dataPolicyId,
            final Class< R > replicationRuleType,
            final FeatureKeyType featureKeyRequired )
    {
        if ( 0 == brm.getRetriever( replicationRuleType ).getCount( Require.all( 
                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                Require.beanPropertyEquals( 
                        DataReplicationRule.TYPE, DataReplicationRuleType.PERMANENT ) ) ) )
        {
            return;
        }
        
        if ( 0 < brm.getRetriever( FeatureKey.class ).getCount( Require.all( 
                Require.beanPropertyEquals( FeatureKey.KEY, featureKeyRequired ),
                Require.beanPropertyEquals( ErrorMessageObservable.ERROR_MESSAGE, null ) ) ) )
        {
            return;
        }
        
        throw new S3RestException( 
                GenericFailure.FEATURE_KEY_REQUIRED,
                "Valid feature key of type " + featureKeyRequired + " is required." );
    }
}
