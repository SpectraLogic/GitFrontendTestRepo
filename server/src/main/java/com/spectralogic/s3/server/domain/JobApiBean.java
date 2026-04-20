/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

public interface JobApiBean extends SimpleBeanSafeToProxy, NameObservable< JobApiBean >
{
    @MarshalXmlAsAttribute
    String getName();
    
    
    String JOB_ID = "jobId";

    @MarshalXmlAsAttribute
    UUID getJobId();
    
    void setJobId( final UUID value );
    
    
    String STATUS = "status";

    @MarshalXmlAsAttribute
    JobStatus getStatus();
    
    void setStatus( final JobStatus value );
    
    
    String NAKED = "naked";

    @MarshalXmlAsAttribute
    boolean isNaked();
    
    void setNaked( final boolean value );
    
    
    String BUCKET_NAME = "bucketName";

    @MarshalXmlAsAttribute
    String getBucketName();
    
    void setBucketName( final String value );
    
    
    String START_DATE = "startDate";

    @MarshalXmlAsAttribute
    Date getStartDate();
    
    void setStartDate( final Date value );
    
    
    String PRIORITY = "priority";

    @MarshalXmlAsAttribute
    BlobStoreTaskPriority getPriority();
    
    void setPriority( final BlobStoreTaskPriority value );
    
    
    String REQUEST_TYPE = "requestType";

    @MarshalXmlAsAttribute
    JobRequestType getRequestType();
    
    void setRequestType( final JobRequestType value );
    
    
    String AGGREGATING = "aggregating";

    @MarshalXmlAsAttribute
    boolean isAggregating();
    
    void setAggregating( final boolean value );
    
    
    String USER_NAME = "userName";

    @MarshalXmlAsAttribute
    String getUserName();
    
    void setUserName( final String value );
    
    
    String USER_ID = "userId";

    @MarshalXmlAsAttribute
    UUID getUserId();
    
    void setUserId( final UUID value );
    
    
    String ORIGINAL_SIZE_IN_BYTES = "originalSizeInBytes";

    @MarshalXmlAsAttribute
    long getOriginalSizeInBytes();
    
    void setOriginalSizeInBytes( final long value );
    
    
    String CACHED_SIZE_IN_BYTES = "cachedSizeInBytes";

    @MarshalXmlAsAttribute
    long getCachedSizeInBytes();
    
    void setCachedSizeInBytes( final long value );
    
    
    String COMPLETED_SIZE_IN_BYTES = "completedSizeInBytes";

    @MarshalXmlAsAttribute
    long getCompletedSizeInBytes();
    
    void setCompletedSizeInBytes( final long value );

    
    String CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE = "chunkClientProcessingOrderGuarantee";
    
    @MarshalXmlAsAttribute
    JobChunkClientProcessingOrderGuarantee getChunkClientProcessingOrderGuarantee();
    
    void setChunkClientProcessingOrderGuarantee( final JobChunkClientProcessingOrderGuarantee value );
    
    
    String NODES = "nodes";
    
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "Node",
            collectionValue = "Nodes",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    NodeApiBean [] getNodes();
    
    void setNodes( final NodeApiBean [] value );
    
    
    String ENTIRELY_IN_CACHE = "entirelyInCache";

    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Boolean getEntirelyInCache();
    
    void setEntirelyInCache( final Boolean value );
}
