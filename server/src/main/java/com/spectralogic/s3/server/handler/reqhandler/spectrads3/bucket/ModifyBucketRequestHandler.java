/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.auth.DataPolicyAuthorization;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyBucketRequestHandler extends BaseModifyBeanRequestHandler< Bucket >
{
    public ModifyBucketRequestHandler()
    {
        super( Bucket.class, 
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.INTERNAL_ONLY,
                    BucketAclPermission.OWNER,
                    AdministratorOverride.YES ),
               RestDomainType.BUCKET );
        
        registerOptionalBeanProperties(
                UserIdObservable.USER_ID,
                Bucket.DATA_POLICY_ID,
                Bucket.PROTECTED );
    }

    
    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final Bucket bucket,
            final Set< String > modifiedProperties )
    {
        final boolean dataPolicyModified = modifiedProperties.remove( Bucket.DATA_POLICY_ID );
        super.modifyBean( params, bucket, modifiedProperties );
        if ( dataPolicyModified )
        {
            DataPolicyAuthorization.verify( params, bucket.getDataPolicyId() );
            params.getDataPolicyResource().modifyBucket(
                    bucket.getId(), bucket.getDataPolicyId() ).get( Timeout.LONG );                    
        }
    }
}
