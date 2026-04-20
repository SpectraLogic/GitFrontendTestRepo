package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;

public interface DestinationSummary extends SimpleBeanSafeToProxy, NameObservable, Identifiable {

    String COMPLETED_CHUNKS = "completedChunks";

    @CustomMarshaledName(
            value = "JobChunk",
            collectionValue = "Complete",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS
    )
    JobChunkApiBean[] getCompletedChunks();

    void setCompletedChunks(final JobChunkApiBean[] completedChunks);


    String INCOMPLETE_CHUNKS = "incompleteChunks";

    @CustomMarshaledName(
            value = "JobChunk",
            collectionValue = "Incomplete",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS
    )
    JobChunkApiBean[] getIncompleteChunks();

    void setIncompleteChunks(final JobChunkApiBean[] incompleteChunks);
}
