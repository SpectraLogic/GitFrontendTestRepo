package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.Map;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public class UndeleteObjectRequestHandler extends BaseDaoTypedRequestHandler< S3Object >{

	public UndeleteObjectRequestHandler()
	{
		super( S3Object.class,
				new BucketAuthorizationStrategy(
		                SystemBucketAccess.INTERNAL_ONLY,
		                BucketAclPermission.DELETE,
		                AdministratorOverride.NO ),
				new RestfulCanHandleRequestDeterminer( RestActionType.BULK_MODIFY, RestDomainType.OBJECT ) );
        registerRequiredBeanProperties( 
                NameObservable.NAME,
                S3Object.BUCKET_ID );
        registerOptionalRequestParameters( RequestParameterType.VERSION_ID );
	}

	@Override
	protected ServletResponseStrategy handleRequestInternal( DS3Request request, CommandExecutionParams params )
	{
		final Map< String, String > beanProperties = request.getBeanPropertyValueMapFromRequestParameters();
		final S3ObjectService objectService = params.getServiceManager().getService( S3ObjectService.class );
		final BucketService bucketService = params.getServiceManager().getService( BucketService.class );
		final Bucket bucket = bucketService.discover( beanProperties.get( S3Object.BUCKET_ID ) );
		
		final S3Object object;
		if ( request.hasRequestParameter( RequestParameterType.VERSION_ID ) )
		{
			object = objectService.retrieve( Require.all(
					Require.beanPropertyEquals( S3Object.NAME, beanProperties.get( S3Object.NAME ) ),
					Require.beanPropertyEquals( S3Object.BUCKET_ID, bucket.getId() ),
					Require.beanPropertyEquals( Identifiable.ID,
							request.getRequestParameter( RequestParameterType.VERSION_ID ).getUuid() ) ) );
			if ( null == object )
			{
				throw new S3RestException( GenericFailure.NOT_FOUND,
	                    "There is no object in bucket " + bucket.getName() + " with name "
                    		+ beanProperties.get( S3Object.NAME ) + " and versionId "
                    		+ request.getRequestParameter( RequestParameterType.VERSION_ID ) );
			}
		}
		else
		{
			object = objectService.getMostRecentVersion( bucket.getId(), beanProperties.get( S3Object.NAME ) );
		}
        
        params.getTargetResource()
        	.undeleteObject( request.getAuthorization().getUserId(), object ).get( Timeout.DEFAULT );
        
        return BeanServlet.serviceModify( params, objectService.attain( object.getId() ) );
	}
}
