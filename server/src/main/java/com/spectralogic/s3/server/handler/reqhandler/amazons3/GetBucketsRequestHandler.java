/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.BucketApiBean;
import com.spectralogic.s3.server.domain.BucketsApiBean;
import com.spectralogic.s3.server.domain.UserApiBeanImpl;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public final class GetBucketsRequestHandler extends BaseRequestHandler
{
    public GetBucketsRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.GET,
                BucketRequirement.NOT_ALLOWED,
                S3ObjectRequirement.NOT_ALLOWED ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final User user = params.getRequest().getAuthorization().getUser();
        if ( user == null )
            LOG.info( "Getting buckets for Internal User " + "'..." );
        else
        	LOG.info( "Getting buckets for '" + user.getName() + "'..." );

        final Set< UUID > bucketIds = BucketAuthorization.getBucketsUserHasAccessTo( 
                SystemBucketAccess.STANDARD, 
                BucketAclPermission.LIST,
                AdministratorOverride.YES,
                params );
        Validations.verifyNotNull( "Shut up CodePro.", bucketIds );
        final List< BucketApiBean > buckets = new ArrayList<>();
        try ( final EnhancedIterable< Bucket > iterable =
                params.getServiceManager().getRetriever( Bucket.class ).retrieveAll(
                        bucketIds ).toIterable() )
        {
            for ( final Bucket b : iterable )
            {
                final BucketApiBean br = BeanFactory.newBean( BucketApiBean.class );
                br.setName( b.getName() );
                br.setCreationDate( b.getCreationDate() );
                buckets.add( br );
            }
        }

        Collections.sort( buckets, new BeanComparator<>( BucketApiBean.class, BucketApiBean.NAME ) );
        final BucketsApiBean retval = BeanFactory.newBean( BucketsApiBean.class );
        retval.setBuckets( CollectionFactory.toArray( BucketApiBean.class, buckets ) );
        if (user != null)
        	retval.setOwner( UserApiBeanImpl.fromUser( user ) );
        
        return BeanServlet.serviceGet( params, retval );
    }
}
