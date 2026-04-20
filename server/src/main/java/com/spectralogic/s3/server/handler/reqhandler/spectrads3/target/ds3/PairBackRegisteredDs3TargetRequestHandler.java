/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class PairBackRegisteredDs3TargetRequestHandler
    extends BaseDaoTypedRequestHandler< Ds3Target >
{
    public PairBackRegisteredDs3TargetRequestHandler()
    {
        super( Ds3Target.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                new RestfulCanHandleRequestDeterminer( 
                        RestOperationType.PAIR_BACK,
                        RestDomainType.DS3_TARGET ) );

        registerOptionalBeanProperties( 
                NameObservable.NAME,
                Ds3Target.ADMIN_AUTH_ID,
                Ds3Target.ADMIN_SECRET_KEY,
                Ds3Target.DATA_PATH_END_POINT,
                Ds3Target.DATA_PATH_HTTPS,
                Ds3Target.DATA_PATH_PORT,
                Ds3Target.DATA_PATH_PROXY,
                Ds3Target.DATA_PATH_VERIFY_CERTIFICATE,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                Ds3Target.ACCESS_CONTROL_REPLICATION,
                Ds3Target.REPLICATED_USER_DEFAULT_DATA_POLICY,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }


    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Ds3Target pairBackTarget = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.YES );
        final DataPathBackend dpb = 
                params.getServiceManager().getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        final Node node = 
                params.getServiceManager().getService( NodeService.class ).getThisNode();
        pairBackTarget.setId( dpb.getInstanceId() );
        if ( null == pairBackTarget.getName() )
        {
            pairBackTarget.setName( node.getName() );
        }
        if ( null == pairBackTarget.getAdminAuthId() )
        {
            pairBackTarget.setAdminAuthId( request.getAuthorization().getUser().getAuthId() );
        }
        if ( null == pairBackTarget.getAdminSecretKey() )
        {
            pairBackTarget.setAdminSecretKey( request.getAuthorization().getUser().getSecretKey() );
        }
        if ( null == pairBackTarget.getDataPathEndPoint() )
        {
            pairBackTarget.setDataPathEndPoint( node.getDnsName() );
            if ( null == node.getDataPathHttpPort() )
            {
                pairBackTarget.setDataPathHttps( true );
                pairBackTarget.setDataPathPort( node.getDataPathHttpsPort() );
            }
            else
            {
                pairBackTarget.setDataPathHttps( false );
                pairBackTarget.setDataPathPort( node.getDataPathHttpPort() );
            }
        }
        if ( null == pairBackTarget.getDataPathEndPoint() )
        {
            pairBackTarget.setDataPathEndPoint( node.getDataPathIpAddress() );
        }
        
        final UUID ds3TargetId = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Ds3Target.class ) ).getId();
        params.getTargetResource().pairBack(
                ds3TargetId,
                pairBackTarget ).get( Timeout.DEFAULT );
        return BeanServlet.serviceCreate( params, null );
    }
}
