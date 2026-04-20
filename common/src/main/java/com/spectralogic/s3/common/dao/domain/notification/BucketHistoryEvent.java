package com.spectralogic.s3.common.dao.domain.notification;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.util.bean.lang.AutoIncrementing;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.notification.domain.SequencedEvent;

import java.util.Date;
import java.util.UUID;

@UniqueIndexes(
{
    @Unique(BucketHistoryEvent.SEQUENCE_NUMBER)
})
public interface BucketHistoryEvent extends SequencedEvent<BucketHistoryEvent> {

    //We override only the getter in order to add the SortBy annotation here instead of in SequencedEvent
    @SortBy
    @AutoIncrementing
    Long getSequenceNumber();

    String OBJECT_NAME = "objectName";

    String getObjectName();

    BucketHistoryEvent setObjectName( final String value );

    
    String OBJECT_CREATION_DATE = "objectCreationDate";

    @Optional
    Date getObjectCreationDate();

    BucketHistoryEvent setObjectCreationDate( final Date value );

    
    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getBucketId();

    BucketHistoryEvent setBucketId( final UUID value );


    String VERSION_ID = "versionId";

    UUID getVersionId();

    BucketHistoryEvent setVersionId( final UUID value );


    String TYPE = "type";

    BucketHistoryEventType getType();

    BucketHistoryEvent setType( final BucketHistoryEventType value );
}
