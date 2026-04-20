package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.sax.SaxHandler;
import com.spectralogic.util.marshal.sax.SaxXmlAttributeParser;
import com.spectralogic.util.marshal.sax.SaxXmlAttributeParser.AttributeIs;
import org.xml.sax.Attributes;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class S3ObjectsToJobApiBeanSaxHandler implements SaxHandler
{
    public S3ObjectsToJobApiBeanSaxHandler( final JobRequestType requestType )
    {
        m_requestType = requestType;
        Validations.verifyNotNull( "Request type", m_requestType );
    }
    
    
    public JobToCreateApiBean getJobToCreate()
    {
        return m_jobToCreate;
    }

    
    public void handleStartElement( final String elementName, final Attributes saxAttributes )
    {
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( elementName, saxAttributes );
        if ( elementName.equalsIgnoreCase( "objects" ) )
        {
            attributes.logAttributesParsed();
        }
        else if ( elementName.equalsIgnoreCase( "object" ) )
        {
            final S3ObjectToJobApiBean bo = BeanFactory.newBean( S3ObjectToJobApiBean.class );
            bo.setName( attributes.getString( S3ObjectToJobApiBean.NAME, AttributeIs.REQUIRED ) );
            final S3ObjectType objectType = S3ObjectType.fromObjectName( bo.getName() );
            if ( JobRequestType.PUT == m_requestType )
            {
                final Long size = attributes.getLong(
                        S3ObjectToJobApiBean.SIZE,
                        ( S3ObjectType.DATA == objectType ) ?
                                AttributeIs.REQUIRED : AttributeIs.OPTIONAL );
                if ( null != size )
                {
                    bo.setSize( size.longValue() );
                }
                if ( S3ObjectType.FOLDER == objectType && 0 != bo.getSize() )
                {
                    throw new S3RestException( 
                            GenericFailure.BAD_REQUEST, 
                            "Folders cannot contain data (size of " + bo.getSize() + " is invalid): " 
                            + bo.getName() );
                }
            }
            if ( JobRequestType.GET == m_requestType || JobRequestType.VERIFY == m_requestType )
            {
                final Long length = attributes.getLong( S3ObjectToJobApiBean.LENGTH, AttributeIs.OPTIONAL );
                if ( null != length )
                {
                    bo.setLength( length.longValue() );
                    if ( 0 > bo.getLength() )
                    {
                        throw new S3RestException( 
                                GenericFailure.BAD_REQUEST, 
                                "Length for " + bo.getName() + " cannot be negative." );
                    }
                }
                final Long offset = attributes.getLong( S3ObjectToJobApiBean.OFFSET, AttributeIs.OPTIONAL );
                if ( null != offset )
                {
                    bo.setOffset( offset.longValue() );
                    if ( 0 > bo.getOffset() )
                    {
                        throw new S3RestException( 
                                GenericFailure.BAD_REQUEST, 
                                "Offset for " + bo.getName() + " cannot be negative." );
                    }
                }
                final UUID versionId = attributes.getUUID( S3ObjectToJobApiBean.VERSION_ID, AttributeIs.OPTIONAL );
                if ( null != versionId )
                {
                    bo.setVersionId( versionId );
                }
            }
            
            m_objectsToJob.add( bo );
        }
        else
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "Invalid XML payload sent by client: Unexpected DOM element: " + elementName );
        }
       
        attributes.verifyAllAttributesHaveBeenConsumed();
    }

    
    public void handleEndElement(final String elementName) 
    {
        if ( elementName.equalsIgnoreCase( "objects" ) )
        {
            m_jobToCreate.setObjects(m_objectsToJob.toArray(new S3ObjectToJobApiBean[0]));
        }
    }

    private final JobToCreateApiBean m_jobToCreate = BeanFactory.newBean( JobToCreateApiBean.class );
    private final List< S3ObjectToJobApiBean > m_objectsToJob = new ArrayList<>();

    private final JobRequestType m_requestType;
}
