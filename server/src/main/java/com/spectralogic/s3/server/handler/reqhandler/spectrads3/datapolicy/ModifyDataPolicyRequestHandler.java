/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyDataPolicyRequestHandler extends BaseModifyBeanRequestHandler< DataPolicy >
{
    public ModifyDataPolicyRequestHandler()
    {
        super( DataPolicy.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_POLICY );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                DataPolicy.BLOBBING_ENABLED,
                DataPolicy.CHECKSUM_TYPE,
                DataPolicy.DEFAULT_BLOB_SIZE,
                DataPolicy.DEFAULT_GET_JOB_PRIORITY,
                DataPolicy.DEFAULT_PUT_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_AFTER_WRITE,
                DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY,
                DataPolicy.VERSIONING,
                DataPolicy.MAX_VERSIONS_TO_KEEP,
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION,
                DataPolicy.ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA );
    }

    
    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final DataPolicy bean,
            final Set< String > modifiedProperties )
    {
        validateBeanToCommit( params, bean, modifiedProperties );
        params.getDataPolicyResource().modifyDataPolicy(
                bean, 
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }

    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final DataPolicy bean,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( DataPolicy.MAX_VERSIONS_TO_KEEP )
                && bean.getMaxVersionsToKeep() <= 0)
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    DataPolicy.MAX_VERSIONS_TO_KEEP
                            + " is '" + bean.getMaxVersionsToKeep()
                            + "' but it has to be greater than zero if specified.");
        }
        
        if ( modifiedProperties.contains( DataPolicy.VERSIONING ) && VersioningLevel.NONE != bean.getVersioning() )
		{
        	if ( 0 < params.getServiceManager().getRetriever( S3Target.class ).getCount(
        			Require.all(
		        			Require.exists(
		        					S3DataReplicationRule.class,
		        					DataReplicationRule.TARGET_ID,
		        					Require.exists(
		        							DataPlacement.DATA_POLICY_ID,
		        							Require.beanPropertyEquals( Identifiable.ID, bean.getId() ) ) ),
							Require.beanPropertyEquals(
									PublicCloudReplicationTarget.NAMING_MODE,
									CloudNamingMode.AWS_S3 ) ) ) )
        	{
        		throw new S3RestException( 
                        GenericFailure.CONFLICT, 
                        CloudNamingMode.AWS_S3 + " naming mode cannot be used with versioning level "
                        		+ bean.getVersioning() + "." );
        	}
		}
    }
}
