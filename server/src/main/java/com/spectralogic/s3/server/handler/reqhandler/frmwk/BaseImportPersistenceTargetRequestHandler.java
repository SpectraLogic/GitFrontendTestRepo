/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;

public abstract class BaseImportPersistenceTargetRequestHandler
    < T extends PersistenceTarget< T > & Identifiable >
    extends BaseDaoTypedRequestHandler< ImportPersistenceTargetDirectiveRequest >
{
    protected BaseImportPersistenceTargetRequestHandler(
            final Class< T > persistenceTargetType,
            final AuthenticationStrategy authenticationStrategy,
            final CanHandleRequestDeterminer canHandleRequestDeterminer )
    {
        super( ImportPersistenceTargetDirectiveRequest.class, 
               authenticationStrategy, 
               canHandleRequestDeterminer );
        m_persistenceTargetType = persistenceTargetType;
                
        registerOptionalBeanProperties( 
                ImportDirective.USER_ID,
                ImportDirective.DATA_POLICY_ID,
                ImportPersistenceTargetDirectiveRequest.PRIORITY,
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID,
                ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT,
                ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT );
    }
    
    
    @Override
    final protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final DataPathBackend dpb = 
                params.getServiceManager().getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        final T pt = ( request.getRestRequest().getAction().isIdApplicable() ) ?
                request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( m_persistenceTargetType ) ) : null;
        final ImportPersistenceTargetDirectiveRequest directive = getBeanSpecifiedViaQueryParameters(
                params, 
                AutoPopulatePropertiesWithDefaults.YES );
        if ( null == directive.getDataPolicyId() && null != directive.getUserId() )
        {
            directive.setDataPolicyId( DataPolicyUtil.getDataPolicy(
                    params, 
                    params.getServiceManager().getRetriever( User.class ).attain( directive.getUserId() ) ) );
        }
        if ( !request.getBeanPropertyValueMapFromRequestParameters().containsKey( 
                ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT ) )
        {
            directive.setVerifyDataPriorToImport( dpb.isDefaultVerifyDataPriorToImport() );
        }
        if ( !request.getBeanPropertyValueMapFromRequestParameters().containsKey( 
                ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT ) )
        {
            directive.setVerifyDataAfterImport( dpb.getDefaultVerifyDataAfterImport() );
        }
        if ( null != directive.getVerifyDataAfterImport() && directive.isVerifyDataPriorToImport() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "Either request data verification prior to import or after import, but not both." );
        }
        if ( null == directive.getPriority() )
        {
            directive.setPriority( BlobStoreTaskPriority.NORMAL );
        }
        
        performImport( params, pt, directive );

        return BeanServlet.serviceModify(
                params, 
                ( null == pt ) ? 
                        null 
                        : params.getServiceManager().getRetriever( m_persistenceTargetType ).attain(
                                pt.getId() ) );
    }
    
    
    protected abstract void performImport(
            final CommandExecutionParams params,
            final T persistenceTarget,
            final ImportPersistenceTargetDirectiveRequest directive );
    
    
    private final Class< T > m_persistenceTargetType;
}
