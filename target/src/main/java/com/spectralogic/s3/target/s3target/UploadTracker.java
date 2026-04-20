package com.spectralogic.s3.target.s3target;

import java.io.ByteArrayInputStream;
import java.util.*;

import org.apache.log4j.Logger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3InitialDataPlacementPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.service.target.S3MultipartUploadService;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.S3ObjectOnCloudInfo;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.s3.target.frmwrk.CloudUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.reflect.ReflectUtil;


public class UploadTracker
{
	public UploadTracker( final S3MultipartUploadService uploadService, final BeansRetriever< Blob > blobRetriever )
    {
        m_uploadService = uploadService;
        m_blobRetriever = blobRetriever;
    }
    
    
    public synchronized void syncUploads(
    		final String cloudBucketName,
    		final S3Client client )
    {
		final Set<String> knownUploads = m_uploadService.getKnownUploadIds();

		ListMultipartUploadsRequest listMultipartUploadsRequest = ListMultipartUploadsRequest.builder()
				.bucket(cloudBucketName)
				.build();

		ListMultipartUploadsResponse multipartUploadListing = client.listMultipartUploads(listMultipartUploadsRequest);
		List<MultipartUpload> uploads = multipartUploadListing.uploads();

		for (final MultipartUpload upload : uploads) {
			if (knownUploads.contains(upload.uploadId())) {
				knownUploads.remove(upload.uploadId());
			} else {
				TargetLogger.LOG.warn("Aborting multipart upload \"" + upload.uploadId() + "\" for object \""
						+ upload.key() + "\" in bucket \"" + cloudBucketName + "\".");
				LOG.info("Aborting stale multipart upload " + upload.uploadId() + ".");

				AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
						.bucket(cloudBucketName)
						.key(upload.key())
						.uploadId(upload.uploadId())
						.build();

				client.abortMultipartUpload(abortRequest);
			}
		}

		for (final String uploadId : knownUploads) {
			TargetLogger.LOG.warn("Removing upload \"" + uploadId + "\" from database since it can no "
					+ "longer be found on cloud.");
			m_uploadService.forgetUpload(uploadId);
		}

	}
	
	
	public synchronized String initBlobWrite(
    		final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Set< S3ObjectProperty > metadata,
            final Object initialDataPlacement,
            S3Client client )
	{
		String uploadId = m_uploads.get(object.getId());
		if (null == uploadId) {
			uploadId = m_uploadService.getUploadIdFor(object.getId());
		}
		if (null == uploadId) {
			TargetLogger.LOG.info("Initializing multipart upload for "
					+ cloudBucket.getName() + "/" + object.getName());

			// Calculate metadata size and prepare metadata map
			Map<String, String> metadataMap = new HashMap<>();
			long metadataSize = 0;
			for (final S3ObjectProperty m : metadata) {
				metadataMap.put(m.getKey(), m.getValue());
				metadataSize += m.getKey().getBytes().length;
				metadataSize += m.getValue().getBytes().length;
			}

			final Set<Blob> blobs = m_blobRetriever.retrieveAll(
					Require.beanPropertyEquals(Blob.OBJECT_ID, object.getId())).toSet();

			final String blobInfo = CloudUtils.getObjectInfoAsString(object.getId(), blobs);
			metadataSize += S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY.getBytes().length;
			metadataSize += blobInfo.getBytes().length;

			if (metadataSize >= 2000) {
				LOG.warn("Metadata size including blob info would be " + metadataSize + "B, "
						+ "exceeding 2KB max. Saving blob info to separate object");
				final String objectInfoOnly = CloudUtils.getObjectInfoAsString(object.getId(), new HashSet<>());
				metadataMap.put(S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY, objectInfoOnly);

				// Put blob info in separate object
				PutObjectRequest putRequest = PutObjectRequest.builder()
						.bucket(cloudBucket.getName())
						.key(S3ObjectOnCloudInfo.getBlobInfoKey(cloudBucket.getOwnerId(), object.getId()))
						.build();

				RequestBody requestBody = RequestBody.fromBytes(blobInfo.getBytes());
				client.putObject(putRequest, requestBody);
			} else {
				metadataMap.put(S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY, blobInfo);
			}

			CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
					.bucket(cloudBucket.getName())
					.key(object.getName())
					.metadata(metadataMap);


			if (null != initialDataPlacement) {
				requestBuilder.storageClass(ReflectUtil.convertEnum(
						S3InitialDataPlacementPolicy.class,
						StorageClass.class,
						(S3InitialDataPlacementPolicy) initialDataPlacement));
			}

			CreateMultipartUploadResponse initiatedUpload = client.createMultipartUpload(requestBuilder.build());
			uploadId = initiatedUpload.uploadId();
			m_uploadService.setUploadId(object.getId(), uploadId);
		}
		m_uploads.put(object.getId(), uploadId);
		return uploadId;

	}
	
	
	public synchronized void completeUploadIfReady(
			final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize, 
            final String uploadId,
            S3Client client )
	{
		if (!m_uploads.containsKey(object.getId())) {
			LOG.info("Multipart upload already completed by another thread. Won't send duplicate request.");
			return;
		}
		ListPartsRequest listPartsRequest = ListPartsRequest.builder()
				.bucket(cloudBucket.getName())
				.key(object.getName())
				.uploadId(uploadId)
				.build();
		ListPartsResponse partsResponse = client.listParts(listPartsRequest);
		List<CompletedPart> completedParts = new ArrayList<>();
		long sizeOnCloud = 0;
		for (Part part : partsResponse.parts()) {
			sizeOnCloud += part.size();
			completedParts.add(CompletedPart.builder()
					.partNumber(part.partNumber())
					.eTag(part.eTag())
					.build());
		}
		if (sizeOnCloud == objectSize && !partsResponse.parts().isEmpty()) {
			TargetLogger.LOG.info("Completing multipart upload for "
					+ cloudBucket.getName() + "/" + object.getName());

			CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder()
					.parts(completedParts)
					.build();

			CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
					.bucket(cloudBucket.getName())
					.key(object.getName())
					.uploadId(uploadId)
					.multipartUpload(multipartUpload)
					.build();

			client.completeMultipartUpload(completeRequest);
			m_uploadService.completeUpload(object.getId(), uploadId);
			m_uploads.remove(object.getId());
		} else if (sizeOnCloud > objectSize) {
			throw new IllegalStateException("More data was uploaded than expected for object \""
					+ object.getName() + "\" in bucket \"" + cloudBucket.getLocalBucketName() + "\"."
					+ " Object size is " + objectSize + " but size of upload is now " + sizeOnCloud + ".");
		}

	}
    
    
    private final S3MultipartUploadService m_uploadService;
    private final BeansRetriever< Blob > m_blobRetriever;
    private final Map< UUID, String > m_uploads = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( UploadTracker.class );
}
