/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.spectralogic.s3.server.domain.S3ObjectsToJobApiBeanParser;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.PhysicalPlacementApiBean;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.platform.domain.BlobApiBeansContainer;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.domain.S3ObjectsToJobApiBeanSaxHandler;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.sax.SaxParser;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class PhysicalPlacementCalculator
{
    PhysicalPlacementCalculator( 
            final UUID storageDomainId,
            final CommandExecutionParams params, 
            final boolean verifyStrictly,
            final boolean requireSuccessfulBlobParsing )
    {
        this( storageDomainId, 
              params, 
              verifyStrictly,
              params.getRequest().getRestRequest().getBean( 
                      params.getServiceManager().getRetriever( Bucket.class ) ),
              extractBlobsFromHttpRequest( 
                      params,
                      params.getRequest().getRestRequest().getBean( 
                              params.getServiceManager().getRetriever( Bucket.class ) ).getId(),
                      verifyStrictly,
                      requireSuccessfulBlobParsing ),
              params.getRequest().hasRequestParameter( RequestParameterType.FULL_DETAILS ),
              false );
    }
    
    
    public PhysicalPlacementCalculator( 
            final UUID storageDomainId,
            final CommandExecutionParams params, 
            final boolean verifyStrictly,
            final Bucket bucket,
            final Set< UUID > blobs,
            final boolean getDetailedBreakdown,
            final boolean onlyIncludeDataLossSuspects )
    {
        m_dpResource = params.getPlannerResource();
        m_storageDomainId = storageDomainId;
        m_onlyIncludeDataLossSuspects = onlyIncludeDataLossSuspects;
        m_brm = params.getServiceManager();
        m_bucket = bucket;
        m_blobs = blobs;
    
        if ( verifyStrictly && ( ( null == m_blobs ) || ( 0 == m_blobs.size() ) ) )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "VerifyPhysicalPlacementForObjects with no objects specified is not supported." );
        }
    
        m_result = ( getDetailedBreakdown ) ? getDetailedResult() : getSummaryResult();
    
        if ( verifyStrictly )
        {
            verifyAllObjectsPersisted( m_brm.getRetriever( S3Object.class )
                                            .retrieveAll( Require.exists( Blob.class, Blob.OBJECT_ID,
                                                    Require.beanPropertyEqualsOneOf( Blob.ID, blobs ) ) )
                                            .toSet() );
        }
    }
    
    
    public static Set< UUID > extractBlobsFromHttpRequest(
            final CommandExecutionParams params, 
            final UUID bucketId,
            final boolean verifyStrictly,
            final boolean requireSuccessfulBlobParsing )
    {
        if ( null == bucketId )
        {
            LOG.info( "No bucket id specified, so will ignore any request payload (if any)." );
            return null;
        }
        
        try
        {
            return getBlobs( params, bucketId, verifyStrictly );
        }
        catch ( final Exception ex )
        {
            if ( FailureTypeObservable.class.isAssignableFrom( ex.getClass() ) )
            {
                throw (FailureTypeObservableException)ex;
            }
            if ( !requireSuccessfulBlobParsing && ExceptionUtil.getReadableMessage( ex ).contains( 
                    "Premature end of file" ) )
            {
                LOG.info( "Blob payload was not provided or was too mangled to parse.  " 
                        + "Will calculate physical placement for bucket "
                        + bucketId + " as a whole.", ex );
                return null;
            }
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "Failed to parse XML request payload.", ex );
        }
    }
    
    
    private static Set< UUID > getBlobs( 
            final CommandExecutionParams params, 
            final UUID bucketId,
            final boolean verifyStrictly ) throws Exception
    {
        WireLogger.LOG.info( "Receiving object specifications from client..." );
        final S3ObjectsToJobApiBeanSaxHandler saxHandler = new S3ObjectsToJobApiBeanSaxHandler( JobRequestType.GET );
        final SaxParser saxParser = new SaxParser();
        saxParser.addHandler( saxHandler );
        saxParser.setInputStream( params.getRequest().getHttpRequest().getInputStream() );
        saxParser.parse();

        final S3ObjectsToJobApiBeanParser beanParser = new S3ObjectsToJobApiBeanParser( params.getServiceManager() );
        
        final Set< Blob > retval = beanParser.parseBlobsToGet( saxHandler.getJobToCreate(), bucketId, verifyStrictly );
        LOG.info( "There are " + retval.size() + " blobs to determine physical placement for." );
        return BeanUtils.toMap( retval ).keySet();
    }
    
    
    private PhysicalPlacementApiBean getSummaryResult()
    {
        final List< Ds3Target > ds3Targets = getSummaryResultDs3Targets();
        final List< AzureTarget > azureTargets = getSummaryResultAzureTargets();
        final List< S3Target > s3Targets = getSummaryResultS3Targets();

        for ( final Ds3Target target : ds3Targets )
        {
            target.setAdminSecretKey( LogUtil.CONCEALED );
        }
        for ( final S3Target target : s3Targets )
        {
            target.setSecretKey( LogUtil.CONCEALED );
        }
        for ( final AzureTarget target : azureTargets )
        {
            target.setAccountKey( LogUtil.CONCEALED );
        }
        
        return BeanFactory.newBean( PhysicalPlacementApiBean.class )
                .setTapes( CollectionFactory.toArray( Tape.class, getSummaryResultTapes() ) )
                .setPools( CollectionFactory.toArray( Pool.class, getSummaryResultPools() ) )
                .setDs3Targets( CollectionFactory.toArray( Ds3Target.class, ds3Targets ) )
                .setAzureTargets( CollectionFactory.toArray( AzureTarget.class, azureTargets) )
                .setS3Targets( CollectionFactory.toArray( S3Target.class, s3Targets ) );
    }
    
    
    private List< Tape > getSummaryResultTapes()
    {
        return getSummaryResult(
                Tape.class, 
                Tape.BAR_CODE,
                BlobTape.class, 
                SuspectBlobTape.class,
                BlobTape.TAPE_ID );
    }
    
    
    private List< Pool > getSummaryResultPools()
    {
        return getSummaryResult(
                Pool.class,
                NameObservable.NAME, 
                BlobPool.class,
                SuspectBlobPool.class,
                BlobPool.POOL_ID );
    }
    
    
    private List< Ds3Target > getSummaryResultDs3Targets()
    {
        if ( null != m_storageDomainId )
        {
            return new ArrayList<>();
        }
        return getSummaryResult(
                Ds3Target.class, 
                NameObservable.NAME, 
                BlobDs3Target.class, 
                SuspectBlobDs3Target.class,
                BlobTarget.TARGET_ID );
    }
    
    
    private List< AzureTarget > getSummaryResultAzureTargets()
    {
        if ( null != m_storageDomainId )
        {
            return new ArrayList<>();
        }
        return getSummaryResult(
                AzureTarget.class, 
                NameObservable.NAME, 
                BlobAzureTarget.class, 
                SuspectBlobAzureTarget.class,
                BlobTarget.TARGET_ID );
    }
    
    
    private List< S3Target > getSummaryResultS3Targets()
    {
        if ( null != m_storageDomainId )
        {
            return new ArrayList<>();
        }
        return getSummaryResult(
                S3Target.class, 
                NameObservable.NAME, 
                BlobS3Target.class, 
                SuspectBlobS3Target.class,
                BlobTarget.TARGET_ID );
    }
    
    
    private < P extends DatabasePersistable, B extends DatabasePersistable > List< P > getSummaryResult(
            final Class< P > persistenceTargetType,
            final String persistenceTargetSortPropName,
            final Class< B > blobPersistenceTargetType,
            final Class< ? extends B > suspectBlobPersistenceTargetType,
            final String blobPersistenceTargetPropName )
    {
        final WhereClause blobFilter = ( null == m_blobs ) ?
                null
                : ( m_onlyIncludeDataLossSuspects ) ? 
                        Require.exists(
                                suspectBlobPersistenceTargetType, 
                                blobPersistenceTargetPropName, 
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, m_blobs ) )
                        : Require.exists(
                                blobPersistenceTargetType, 
                                blobPersistenceTargetPropName, 
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, m_blobs ) );
        final WhereClause sdFilter = ( null == m_storageDomainId ) ?
                null
                : Require.exists(
                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                        Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, m_storageDomainId ) );
        final WhereClause bucketFilter = ( null == m_bucket ) ?
                null
                : Require.exists(
                        blobPersistenceTargetType,
                        blobPersistenceTargetPropName, 
                        Require.exists(
                                BlobObservable.BLOB_ID,
                                Require.exists(
                                        Blob.OBJECT_ID,
                                        Require.beanPropertyEquals( 
                                                S3Object.BUCKET_ID, 
                                                m_bucket.getId() ) ) ) );
        
        final List< P > retval = m_brm.getRetriever( persistenceTargetType ).retrieveAll( 
                Require.all( sdFilter, bucketFilter, blobFilter ) ).toList();
        retval.sort( new BeanComparator<>( persistenceTargetType, persistenceTargetSortPropName ) );
        return retval;
    }
    
    
    private BlobApiBeansContainer getDetailedResult()
    {
        if ( null == m_blobs )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "Cannot ask for " + RequestParameterType.FULL_DETAILS 
                    + " without specifying an object payload to get full details for." );
        }
        
        final Set< Blob > blobs = m_brm.getRetriever( Blob.class ).retrieveAll( m_blobs ).toSet();
        final Set< UUID > blobIdsInCache = CollectionFactory.toSet( m_dpResource.getBlobsInCache( 
                CollectionFactory.toArray( UUID.class, BeanUtils.toMap( blobs ).keySet() ) ).get(
                        Timeout.DEFAULT ).getBlobsInCache() );
        return new BlobApiBeanBuilder( 
                m_brm.getRetriever( Bucket.class ), 
                m_brm.getRetriever( S3Object.class ), 
                blobs )
            .includeBlobCacheState( blobIdsInCache )
            .includePhysicalPlacement( m_brm, m_onlyIncludeDataLossSuspects )
            .filterToStorageDomain( m_storageDomainId ).buildAndWrap();
    }
    
    
    private void verifyAllObjectsPersisted( final Set< S3Object > objects )
    {
        final Set< S3Object > objectsNotPersisted = objects.stream()
                                                           .filter(
                                                                   object -> !PersistenceTargetUtil
                                                                           .isObjectFullyPersisted(
                                                                           object.getId(), m_brm ) )
                                                           .collect( Collectors.toSet() );
    
        if ( 0 == objectsNotPersisted.size() )
        {
            return;
        }
    
        throw new S3RestException( GenericFailure.NOT_FOUND,
                objectsNotPersisted.size() + " objects are not physically placed: " + objectsNotPersisted.stream()
                                                                                                         .map( S3Object::getName )
                                                                                                         .collect(
                                                                                                                 Collectors.joining(
                                                                                                                         ", " ) ) );
    }
    
    
    public Object getResult()
    {
        return m_result;
    }
    
    
    private final BeansServiceManager m_brm;
    private final Set< UUID > m_blobs;
    private final Object m_result;
    private final boolean m_onlyIncludeDataLossSuspects;
    private final UUID m_storageDomainId;
    private final Bucket m_bucket;
    private final DataPlannerResource m_dpResource;
    
    private final static Logger LOG = Logger.getLogger( PhysicalPlacementCalculator.class );
}