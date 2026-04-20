/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface S3ObjectOnMedia extends SimpleBeanSafeToProxy, Identifiable
{
    String OBJECT_NAME = "objectName";
    
    /**
     * @return the object name portion of the object (not including the bucket name).  For example, for the 
     * file name '/bucket1/folder/data.jpg' on LTFS, the bucket name would be 'bucket1', and the object name 
     * (this attribute) would be 'folder/data.jpg'
     */
    String getObjectName();
    
    void setObjectName( final String value );
        

    String BLOBS = "blobs";
    
    /**
     * @return 1 or more blobs if the object has any data associated with it, or a single zero-length blob if
     * the object has no data associated with it  <br><br>
     * 
     * Note: An object has no data associated with it if either (i) the object name ends with 
     * {@link S3Object#DELIMITER} (the object is a {@link S3ObjectType#FOLDER}), or (ii) the object is a
     * zero-length object.  Note that, in both of these scenarios, there may be metadata associated with the
     * object.
     */
    BlobOnMedia [] getBlobs();
    
    void setBlobs( final BlobOnMedia [] value );
    
    
    String METADATA = "metadata";
    
    /**
     * Object metadata is immutable and cannot be modified once the writing of the object begins.
     */
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    S3ObjectMetadataKeyValue [] getMetadata();
    
    void setMetadata( final S3ObjectMetadataKeyValue [] value );
}
