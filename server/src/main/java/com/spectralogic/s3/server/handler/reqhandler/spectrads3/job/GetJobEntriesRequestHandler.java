/*******************************************************************************
 *
 * Copyright C 2025, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetJobEntriesRequestHandler extends BaseGetBeansRequestHandler< JobEntry >
{
    public GetJobEntriesRequestHandler()
    {
        super( JobEntry.class,
               new BucketAuthorizationStrategy(
                       SystemBucketAccess.STANDARD,
                       BucketAclPermission.JOB,
                       AdministratorOverride.YES ),
               RestDomainType.JOB_CHUNK_DAO );

        registerRequiredBeanProperties( JobEntry.JOB_ID );
    }


    @Override
    protected WhereClause getCustomFilter( final JobEntry requestBean, final CommandExecutionParams params )
    {
        params.getPlannerResource().cleanUpCompletedJobsAndJobChunks().get( Timeout.DEFAULT );

        if ( null != requestBean.getJobId() )
        {
            return Require.beanPropertyEquals( JobEntry.JOB_ID, requestBean.getJobId() );
        }

        return Require.nothing();
    }
}
