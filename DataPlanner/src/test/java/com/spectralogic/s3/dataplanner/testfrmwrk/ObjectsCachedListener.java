package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsCachedNotificationPayload;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.domain.Notification;
import com.spectralogic.util.testfrmwrk.TestPerfMonitor;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ObjectsCachedListener implements NotificationListener {
    Set<UUID> jobIds = new HashSet<>();

    @Override
    public void fire(final Notification event) {
        TestPerfMonitor.hit("Received notification" + event.getType());
        final S3ObjectsCachedNotificationPayload payload =
                (S3ObjectsCachedNotificationPayload) event.getEvent();
        jobIds.add(payload.getJobId());
    }

    public Set<UUID> getJobIds() {
        return jobIds;
    }
}
