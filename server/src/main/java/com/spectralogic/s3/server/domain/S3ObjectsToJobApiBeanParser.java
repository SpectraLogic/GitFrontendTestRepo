package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;

import java.util.*;

public class S3ObjectsToJobApiBeanParser {

    public S3ObjectsToJobApiBeanParser( final BeansRetrieverManager brm ) {
        Validations.verifyNotNull( "Beans retriever manager", brm );

        m_blobRetriever = brm.getRetriever( Blob.class );
        m_objectRetriever = brm.getRetriever( S3Object.class );
    }

    public Set<UUID> parseBlobIdsToGet( final JobToCreateApiBean jobToCreate )
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final S3ObjectToJobApiBean bo : jobToCreate.getObjects() )
        {
            retval.add( UUID.fromString( bo.getName() ) );
        }

        return retval;
    }

    public Set<Blob> parseBlobsToGet(
            final JobToCreateApiBean jobToCreate,
            final UUID bucketId,
            final boolean failIfOneOrMoreObjectsDoesNotExist )
    {

        final Set< Blob > blobSet = new HashSet<>();
        final Set< String > missingObjectSet = new HashSet<>();
        for ( S3ObjectToJobApiBean o : jobToCreate.getObjects() )
        {
            final UUID versionId = o.getVersionId();
            final WhereClause versionFilter;
            if ( null != versionId )
            {
                versionFilter = Require.beanPropertyEquals( Identifiable.ID, versionId );
            }
            else
            {
                versionFilter = Require.beanPropertyEquals( S3Object.LATEST, Boolean.TRUE );
            }
            final WhereClause rangeFilter;
            if ( 0 != o.getLength() )
            {
                rangeFilter = Require.all(
                        Require.beanPropertiesSumGreaterThan(
                                Blob.BYTE_OFFSET, Blob.LENGTH,
                                Long.valueOf( o.getOffset() ) ),
                        Require.beanPropertyLessThan(
                                Blob.BYTE_OFFSET,
                                Long.valueOf( o.getOffset() + o.getLength() ) ) );
            }
            else
            {
                //NOTE: this excludes the possibility of someone explicitly requesting only offset 0
                rangeFilter = Require.nothing();
            }

            final List< Blob > blobsForObject = m_blobRetriever.retrieveAll( Require.all(
                    rangeFilter,
                    Require.exists( Blob.OBJECT_ID,
                            Require.all(
                                    Require.beanPropertyEquals( S3Object.NAME, o.getName() ),
                                    Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ),
                                    versionFilter ) ) ) ).toList();
            if ( blobsForObject.isEmpty() )
            {
                if ( 0 > o.getOffset() || objectFound( o, bucketId, versionFilter ) )
                {
                    throw new S3RestException(
                            GenericFailure.BAD_REQUEST,
                            "Object exists, but byte range is invalid for: " + getObjectString( o ) );
                }
                else
                {
                    missingObjectSet.add( getObjectString( o ) );
                }
            }
            else if ( o.getLength() != 0 )
            {
                Collections.sort( blobsForObject, new S3ObjectsToJobApiBeanParser.BlobOffsetComparator() );
                Blob cur = blobsForObject.get( 0 );
                if ( cur.getByteOffset() > o.getOffset() )
                {
                    /* since we have already checked for negative offsets, this will only happen if we are missing
                     * blob 0 for this object */
                    missingObjectSet.add( getObjectString( o ) );
                }
                for ( int i = 1; i < blobsForObject.size(); i++ )
                {
                    if ( cur.getByteOffset() + cur.getLength() != blobsForObject.get( i ).getByteOffset() )
                    {
                        //contiguous blobs are not available for the requested range
                        missingObjectSet.add( getObjectString( o ) );
                    }
                    cur = blobsForObject.get( i );
                }
                if ( cur.getByteOffset() + cur.getLength() < o.getOffset() + o.getLength() )
                {
                    throw new S3RestException(
                            GenericFailure.BAD_REQUEST,
                            "Object exists, but byte range is invalid for: " + getObjectString( o ) );
                }
            }
            blobSet.addAll( blobsForObject );
        }
        if ( !missingObjectSet.isEmpty() && failIfOneOrMoreObjectsDoesNotExist )
        {
            throw new S3RestException(
                    GenericFailure.NOT_FOUND,
                    "Could not find requested blobs for: " +
                            LogUtil.getShortVersion( missingObjectSet.toString() ) );
        }
        return blobSet;
    }

    private boolean objectFound( final S3ObjectToJobApiBean o, final UUID bucketId, WhereClause versionFilter )
    {
        return null != m_objectRetriever.retrieve(
                Require.all(
                        Require.beanPropertyEquals( S3Object.NAME, o.getName() ),
                        Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ),
                        versionFilter ) );
    }

    private String getObjectString( final S3ObjectToJobApiBean o )
    {
        String missingObject = o.getName();
        if ( null != o.getVersionId() )
        {
            missingObject += " - version " + o.getVersionId();
        }
        if ( 0 != o.getOffset() )
        {
            missingObject += " - offset " + o.getOffset();
        }
        if ( 0 != o.getLength() )
        {
            missingObject += " - length " + o.getLength();
        }
        return missingObject;
    }

    private static class BlobOffsetComparator implements Comparator< Blob >
    {
        @Override
        public int compare(Blob o1, Blob o2)
        {
            if ( o1.getByteOffset() < o2.getByteOffset() )
            {
                return -1;
            }
            else if ( o1.getByteOffset() == o2.getByteOffset() )
            {
                return 0;
            }
            return 1;
        }
    } // end inner class def

    private final BeansRetriever< Blob > m_blobRetriever;
    private final BeansRetriever< S3Object > m_objectRetriever;
}
