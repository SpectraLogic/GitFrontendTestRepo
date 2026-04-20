package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.S3MultipartUpload;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3MultipartUploadService extends BeansRetriever< S3MultipartUpload >
{
	String getUploadIdFor( final UUID objectId );
	
	
	void setUploadId( final UUID objectId, final String uploadId );
	
	
	Set< String > getKnownUploadIds();
	
	
	void completeUpload( final UUID objectId, final String uploadId );
	
	
	void forgetUpload( final String uploadId );
}
