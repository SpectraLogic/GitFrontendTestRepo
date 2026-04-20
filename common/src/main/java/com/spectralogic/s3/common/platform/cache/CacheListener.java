package com.spectralogic.s3.common.platform.cache;

import java.util.UUID;

public interface CacheListener
{
	void blobLoadedToCache( final UUID blobId );
}
