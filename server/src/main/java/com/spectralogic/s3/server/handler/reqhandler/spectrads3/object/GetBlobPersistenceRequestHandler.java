/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.platform.spectrads3.BlobIdsSpecification;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.JsonMarshaler;

public final class GetBlobPersistenceRequestHandler extends BaseDaoTypedRequestHandler< BlobIdsSpecification >
{
    public GetBlobPersistenceRequestHandler()
    {
        super( BlobIdsSpecification.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST, 
                       RestDomainType.BLOB_PERSISTENCE ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BlobIdsSpecification requestPayload;
        try
        {
            requestPayload = JsonMarshaler.unmarshal(
                    BlobIdsSpecification.class, 
                    IOUtils.toString( request.getHttpRequest().getInputStream(), Charset.defaultCharset() ) );
            WireLogger.LOG.info( 
                    "Parsed " + requestPayload.getBlobIds().length + " blob ids from request payload." );
        }
        catch ( final Exception ex )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "Cannot parse blob id specification from client.", ex );
        }
        
        final Map< UUID, BlobPersistence > retval = new HashMap<>();
        for ( final Blob blob : params.getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                CollectionFactory.toSet( requestPayload.getBlobIds() ) ).toSet() )
        {
            final BlobPersistence bbi = BeanFactory.newBean( BlobPersistence.class );
            bbi.setChecksum( blob.getChecksum() );
            bbi.setChecksumType( blob.getChecksumType() );
            bbi.setId( blob.getId() );
            retval.put( blob.getId(), bbi );
        }
        for ( final BlobTape bt : PersistenceTargetUtil.findBlobTapesAvailableNow(
                params.getServiceManager().getRetriever( BlobTape.class ),
                retval.keySet(),
                false,
                null ) )
        {
            retval.get( bt.getBlobId() ).setAvailableOnTapeNow( true );
        }
        for ( final BlobPool bp : PersistenceTargetUtil.findBlobPoolsAvailableNow(
                params.getServiceManager().getRetriever( BlobPool.class ),
                retval.keySet(),
                null,
                false ) )
        {
            retval.get( bp.getBlobId() ).setAvailableOnPoolNow( true );
        }
        
        final BlobPersistenceContainer container = BeanFactory.newBean( BlobPersistenceContainer.class );
        container.setBlobs( CollectionFactory.toArray( BlobPersistence.class, retval.values() ) );
        container.setJobExistant( ( null == requestPayload.getJobId() ) ?
                false 
                : null != params.getServiceManager().getRetriever( Job.class ).retrieve( 
                        requestPayload.getJobId() ) );

        return BeanServlet.serviceGet( 
                params,
                container.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ) );
    }
}
