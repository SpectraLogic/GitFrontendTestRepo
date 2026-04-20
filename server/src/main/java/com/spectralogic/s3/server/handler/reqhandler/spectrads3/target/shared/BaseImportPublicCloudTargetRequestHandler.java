/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.DataPolicyUtil;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;

public abstract class BaseImportPublicCloudTargetRequestHandler
    < T extends PublicCloudReplicationTarget< T > & DatabasePersistable, 
      ID extends ImportPublicCloudTargetDirective< ID > & DatabasePersistable >
    extends BaseDaoTypedRequestHandler< ID >
{
    protected BaseImportPublicCloudTargetRequestHandler(
            final Class< T > targetType,
            final Class< ID > importDirectiveType,
            final AuthenticationStrategy authenticationStrategy,
            final CanHandleRequestDeterminer canHandleRequestDeterminer )
    {
        super( importDirectiveType, authenticationStrategy, canHandleRequestDeterminer );
        m_targetType = targetType;
        
        registerRequiredBeanProperties( 
                ImportPublicCloudTargetDirective.CLOUD_BUCKET_NAME );
        registerOptionalBeanProperties( 
                ImportDirective.USER_ID,
                ImportDirective.DATA_POLICY_ID,
                ImportPublicCloudTargetDirective.PRIORITY );
    }
    
    
    @Override
    final protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final DataPathBackend dpb = 
                params.getServiceManager().getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        final ID directive = getBeanSpecifiedViaQueryParameters(
                params, 
                AutoPopulatePropertiesWithDefaults.YES );
        if ( null == directive.getDataPolicyId() && null != directive.getUserId() )
        {
            directive.setDataPolicyId( DataPolicyUtil.getDataPolicy(
                    params, 
                    params.getServiceManager().getRetriever( User.class ).attain( directive.getUserId() ) ) );
        }
        directive.setTargetId(
                request.getRestRequest().getId( params.getServiceManager().getRetriever( m_targetType ) ) );
        
        directive.setId( UUID.randomUUID() );
        performImport( params, directive );

        return BeanServlet.serviceModify(
                params, 
                null );
    }
    
    
    protected abstract void performImport(
            final CommandExecutionParams params,
            final ID directive );
    
    
    private final Class< T > m_targetType;
}
