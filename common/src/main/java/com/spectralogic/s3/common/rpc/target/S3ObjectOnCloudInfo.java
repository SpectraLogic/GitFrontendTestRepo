package com.spectralogic.s3.common.rpc.target;

import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

import java.util.UUID;

public interface S3ObjectOnCloudInfo  extends SimpleBeanSafeToProxy, Identifiable
{
	String BLOBS = "blobs";

	BlobOnMedia [] getBlobs();
    
    void setBlobs( final BlobOnMedia [] value );
    
    final static String OBJECT_INFO_META_KEY = KeyValueObservable.UNPROTECTED_SPECTRA_KEY_NAMESPACE + "object-info";

    final static String BLOB_INFO_OBJECT_PREFIX = "spectra-blob-info.";

    static String getBlobInfoKey( final UUID ownerId, final UUID objectId ) {
        return BLOB_INFO_OBJECT_PREFIX + ownerId + "/" + objectId;
    }
}
