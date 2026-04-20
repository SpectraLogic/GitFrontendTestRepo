package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( S3MultipartUpload.OBJECT_ID ),
    @Unique( S3MultipartUpload.UPLOAD_ID )
})
public interface S3MultipartUpload extends DatabasePersistable
{
	String OBJECT_ID = "objectId";
    
    @References( S3Object.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getObjectId();
    
    S3MultipartUpload setObjectId( final UUID value );
    
    
    String UPLOAD_ID = "uploadId";
    
    String getUploadId();
    
    S3MultipartUpload setUploadId( final String value ); 
}
