package com.spectralogic.s3.common.rpc.dataplanner;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

import java.util.UUID;

public interface DiskFileInfo extends SimpleBeanSafeToProxy {
    /**
     * Returns the file system absolute path for the blob
     */
    String FILE_PATH = "filePath";

    String getFilePath();
    DiskFileInfo setFilePath(final String value);

    /**
     * Returns the id of the associated blob pool if one exists
     */
    String BLOB_POOL_ID = "blobPoolId";

    @Optional
    UUID getBlobPoolId();
    DiskFileInfo setBlobPoolId(final UUID value);


    /**
     * Returns whether the blob file is located on cache or pool
     */
    static String source(final DiskFileInfo info) {
        return info.getBlobPoolId() == null ? "cache" : "pool";
    }

}