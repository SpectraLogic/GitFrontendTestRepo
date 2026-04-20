package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.S3MultipartUpload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;

class S3MultipartUploadServiceImpl extends BaseService< S3MultipartUpload > implements S3MultipartUploadService
{
	protected S3MultipartUploadServiceImpl()
	{
		super( S3MultipartUpload.class );
	}


	public String getUploadIdFor( final UUID objectId )
	{
		final S3MultipartUpload upload = retrieve( Require.beanPropertyEquals( S3MultipartUpload.OBJECT_ID, objectId ) );
		if ( null == upload )
		{
			return null;
		}
		return upload.getUploadId();
	}


	public void setUploadId( final UUID objectId, final String uploadId )
	{
		create( BeanFactory.newBean( S3MultipartUpload.class ).setObjectId( objectId ).setUploadId( uploadId ) );
	}


	public void completeUpload( final UUID objectId, final String uploadId )
	{
		final S3MultipartUpload upload = attain(
				Require.all(
						Require.beanPropertyEquals( S3MultipartUpload.OBJECT_ID, objectId ),
						Require.beanPropertyEquals( S3MultipartUpload.UPLOAD_ID, uploadId ) ) );
		delete( upload.getId() );
	}
	
	
	public Set< String > getKnownUploadIds()
	{
		return BeanUtils.extractPropertyValues( retrieveAll().toSet(), S3MultipartUpload.UPLOAD_ID );
	}
	
	
	public void forgetUpload( final String uploadId )
	{
		delete( Require.beanPropertyEquals( S3MultipartUpload.UPLOAD_ID, uploadId ) );
	}
}
