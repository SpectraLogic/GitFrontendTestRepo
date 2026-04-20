package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface ReceivedBlob extends ChecksumObservable< ReceivedBlob >, SimpleBeanSafeToProxy
{
    String SIZE_READ = "sizeRead";

    public void setSizeRead( final long size );
    
    public long getSizeRead();

    
    String FILE_NAME = "fileName";
    
    public void setFileName( final String filename );
    
    public String getFileName();
}
