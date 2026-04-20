/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.util.notification.domain.SequencedEvent;
import com.spectralogic.util.notification.domain.SequencedNotificationPayload;

public interface BucketChangesNotificationPayload extends SequencedNotificationPayload<BucketHistoryEvent>
{
}