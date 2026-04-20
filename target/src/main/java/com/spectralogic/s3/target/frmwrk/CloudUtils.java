package com.spectralogic.s3.target.frmwrk;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.target.S3ObjectOnCloudInfo;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.JsonMarshaler;

public class CloudUtils
{
	public static S3ObjectOnCloudInfo getObjectInfo( final UUID objectId, final Set< Blob > blobs )
	{
		final S3ObjectOnCloudInfo retval = BeanFactory.newBean( S3ObjectOnCloudInfo.class ); 
		final BlobOnMedia[] blobArray = new BlobOnMedia[ blobs.size() ];
		int i = 0;
		for ( final Blob b : blobs )
		{
			blobArray[i] = BeanFactory.newBean( BlobOnMedia.class );
			blobArray[i].setId( b.getId() );
			blobArray[i].setOffset( b.getByteOffset() );
			blobArray[i].setLength( b.getLength() );
			blobArray[i].setChecksumType( b.getChecksumType() );
			blobArray[i].setChecksum( b.getChecksum() );
			i++;
		}
		retval.setBlobs( blobArray );
		retval.setId( objectId );
		return retval;
	}
	
	
	public static String getObjectInfoAsString( final UUID objectId, final Set< Blob > blobs )
	{
		return getObjectInfoAsString( getObjectInfo( objectId , blobs ) );
	}
	
	
	public static String getObjectInfoAsString( S3ObjectOnCloudInfo info )
	{
        return JsonMarshaler.marshal( info, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );        
	}
	
	
	public static S3ObjectOnCloudInfo getObjectInfoFromString( final String info )
	{
    	return JsonMarshaler.unmarshal( S3ObjectOnCloudInfo.class, info );
	}
}
