package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.orm.*;
import com.spectralogic.s3.server.domain.*;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;

import java.util.*;

public class GetAbmConfigRequestHandler extends BaseRequestHandler {

    public GetAbmConfigRequestHandler() {
        super(new DefaultPublicExposureAuthenticationStrategy(DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.ADMINISTRATOR),
                new RestfulCanHandleRequestDeterminer(
                        RestActionType.LIST,
                        RestDomainType.ABM_CONFIG));
    }

    @Override
    protected ServletResponseStrategy handleRequestInternal(DS3Request request, CommandExecutionParams params) {
        final AbmConfigApiBean retval = BeanFactory.newBean(AbmConfigApiBean.class);
        final BeansServiceManager serviceManager = params.getServiceManager();

        // Populate all configuration sections
        final Set<UUID> usedStorageDomainIds = new HashSet<>();
        retval.setDataPolicies(populateDataPolicies(serviceManager, usedStorageDomainIds));
        retval.setStorageDomains(populateStorageDomains(serviceManager, usedStorageDomainIds));
        retval.setTapePartitions(populateTapePartitions(serviceManager));
        retval.setPoolPartitions(populatePoolPartitions(serviceManager));
        retval.setTargets(populateTargets(serviceManager));
        retval.setMessage("NOTICE: this summary omits data for clarity, including many parameters when they are at their default values, as well as data policies and storage domains that are unused. Buckets listed are limited to 100 per data policy.");

        return BeanServlet.serviceGet(
                params,
                retval);
    }

    private DataPolicyApiBean[] populateDataPolicies(final BeansServiceManager serviceManager, final Set<UUID> usedStorageDomainIds) {
        final List<DataPolicyApiBean> dataPolicyApiBeans = new ArrayList<>();
        final List<DataPolicy> allDataPolicies = serviceManager.getRetriever(DataPolicy.class).retrieveAll().toList();

        for (final DataPolicy dataPolicy : allDataPolicies) {
            final DataPolicyRM dataPolicyRM = new DataPolicyRM(dataPolicy, serviceManager);
            final List<Bucket> buckets = dataPolicyRM.getBuckets().toList();

            // Only include data policies that have buckets
            if (!buckets.isEmpty()) {
                final DataPolicyApiBean dataPolicyApiBean = BeanFactory.newBean(DataPolicyApiBean.class);
                BeanCopier.copy(dataPolicyApiBean, dataPolicy);

                // Add buckets
                populateBuckets(dataPolicyApiBean, buckets);

                // Add data persistence rules and collect storage domain IDs
                usedStorageDomainIds.addAll(populateDataPersistenceRules(dataPolicyApiBean, dataPolicyRM, serviceManager));

                // Add data replication rules
                populateDataReplicationRules(dataPolicyApiBean, dataPolicyRM, serviceManager);

                dataPolicyApiBeans.add(dataPolicyApiBean);
            }
        }

        return dataPolicyApiBeans.isEmpty() ? null : CollectionFactory.toArray(DataPolicyApiBean.class, dataPolicyApiBeans);
    }

    private void populateBuckets(final DataPolicyApiBean dataPolicyApiBean, final List<Bucket> buckets) {
        final List<HumanReadableBucketApiBean> bucketApiBeans = new ArrayList<>();
        final int maxBuckets = Math.min(buckets.size(), 100);
        
        for (int i = 0; i < maxBuckets; i++) {
            final Bucket bucket = buckets.get(i);
            final HumanReadableBucketApiBean bucketApiBean = BeanFactory.newBean(HumanReadableBucketApiBean.class);
            BeanCopier.copy(bucketApiBean, bucket);
            bucketApiBeans.add(bucketApiBean);
        }
        dataPolicyApiBean.setBuckets(CollectionFactory.toArray(HumanReadableBucketApiBean.class, bucketApiBeans));
    }

    private Set<UUID> populateDataPersistenceRules(final DataPolicyApiBean dataPolicyApiBean, final DataPolicyRM dataPolicyRM, final BeansServiceManager serviceManager) {
        final List<DataPersistenceRule> persistenceRules = dataPolicyRM.getDataPersistenceRules().toList();
        final Set<UUID> usedStorageDomainIds = new HashSet<>();
        if (!persistenceRules.isEmpty()) {
            final List<DataPersistenceRuleApiBean> persistenceRuleApiBeans = new ArrayList<>();
            for (final DataPersistenceRule persistenceRule : persistenceRules) {
                final DataPersistenceRuleApiBean persistenceRuleApiBean = BeanFactory.newBean(DataPersistenceRuleApiBean.class);
                BeanCopier.copy(persistenceRuleApiBean, persistenceRule);

                // Set storage domain name
                final StorageDomain storageDomain = serviceManager.getRetriever(StorageDomain.class).retrieve(persistenceRule.getStorageDomainId());
                usedStorageDomainIds.add(persistenceRule.getStorageDomainId());
                if (storageDomain != null) {
                    persistenceRuleApiBean.setStorageDomainName(storageDomain.getName());
                }

                persistenceRuleApiBeans.add(persistenceRuleApiBean);
            }
            dataPolicyApiBean.setDataPersistenceRules(CollectionFactory.toArray(DataPersistenceRuleApiBean.class, persistenceRuleApiBeans));
        } else {
            dataPolicyApiBean.setDataPersistenceRules(null);
        }
        return usedStorageDomainIds;
    }

    private void populateDataReplicationRules(final DataPolicyApiBean dataPolicyApiBean, final DataPolicyRM dataPolicyRM, final BeansServiceManager serviceManager) {
        final List<DataReplicationRuleApiBean> replicationRuleApiBeans = new ArrayList<>();

        // Add DS3 replication rules
        final List<Ds3DataReplicationRule> ds3ReplicationRules = dataPolicyRM.getDs3DataReplicationRules().toList();
        for (final Ds3DataReplicationRule ds3Rule : ds3ReplicationRules) {
            final DataReplicationRuleApiBean replicationRuleApiBean = BeanFactory.newBean(DataReplicationRuleApiBean.class);
            BeanCopier.copy(replicationRuleApiBean, ds3Rule);

            // Set DS3 target name
            if (ds3Rule.getTargetId() != null) {
                final Ds3Target ds3Target = serviceManager.getRetriever(Ds3Target.class).retrieve(ds3Rule.getTargetId());
                if (ds3Target != null) {
                    replicationRuleApiBean.setTargetName(ds3Target.getName());
                }
            }

            replicationRuleApiBeans.add(replicationRuleApiBean);
        }

        // Add S3 replication rules
        final List<S3DataReplicationRule> s3ReplicationRules = dataPolicyRM.getS3DataReplicationRules().toList();
        for (final S3DataReplicationRule s3Rule : s3ReplicationRules) {
            final DataReplicationRuleApiBean replicationRuleApiBean = BeanFactory.newBean(DataReplicationRuleApiBean.class);
            BeanCopier.copy(replicationRuleApiBean, s3Rule);

            // Set S3 target name
            if (s3Rule.getTargetId() != null) {
                final S3Target s3Target = serviceManager.getRetriever(S3Target.class).retrieve(s3Rule.getTargetId());
                if (s3Target != null) {
                    replicationRuleApiBean.setTargetName(s3Target.getName());
                }
            }

            replicationRuleApiBeans.add(replicationRuleApiBean);
        }

        // Add Azure replication rules
        final List<AzureDataReplicationRule> azureReplicationRules = dataPolicyRM.getAzureDataReplicationRules().toList();
        for (final AzureDataReplicationRule azureRule : azureReplicationRules) {
            final DataReplicationRuleApiBean replicationRuleApiBean = BeanFactory.newBean(DataReplicationRuleApiBean.class);
            BeanCopier.copy(replicationRuleApiBean, azureRule);

            // Set Azure target name
            if (azureRule.getTargetId() != null) {
                final AzureTarget azureTarget = serviceManager.getRetriever(AzureTarget.class).retrieve(azureRule.getTargetId());
                if (azureTarget != null) {
                    replicationRuleApiBean.setTargetName(azureTarget.getName());
                }
            }

            replicationRuleApiBeans.add(replicationRuleApiBean);
        }

        // Set the replication rules array, or null if empty
        dataPolicyApiBean.setDataReplicationRules(replicationRuleApiBeans.isEmpty() ? null : CollectionFactory.toArray(DataReplicationRuleApiBean.class, replicationRuleApiBeans));
    }

    private StorageDomainApiBean[] populateStorageDomains(final BeansServiceManager serviceManager, Set<UUID> usedStorageDomainIds) {
        final List<StorageDomainApiBean> storageDomainApiBeans = new ArrayList<>();
        final List<StorageDomain> allStorageDomains = serviceManager.getRetriever(StorageDomain.class).retrieveAll().toList();

        for (final StorageDomain storageDomain : allStorageDomains) {
            // Only include storage domains that are used by data persistence rules
            if (!usedStorageDomainIds.contains(storageDomain.getId())) {
                continue;
            }

            final StorageDomainRM storageDomainRM = new StorageDomainRM(storageDomain, serviceManager);
            final List<StorageDomainMember> members = storageDomainRM.getStorageDomainMembers().toList();

            final StorageDomainApiBean storageDomainApiBean = BeanFactory.newBean(StorageDomainApiBean.class);
            BeanCopier.copy(storageDomainApiBean, storageDomain);

            // Add storage domain members
            if (!members.isEmpty()) {
                final List<StorageDomainMemberApiBean> memberApiBeans = new ArrayList<>();
                for (final StorageDomainMember member : members) {
                    final StorageDomainMemberApiBean memberApiBean = BeanFactory.newBean(StorageDomainMemberApiBean.class);
                    BeanCopier.copy(memberApiBean, member);

                    // Set partition name
                    if (member.getTapePartitionId() != null) {
                        final TapePartition tapePartition = serviceManager.getRetriever(TapePartition.class).retrieve(member.getTapePartitionId());
                        if (tapePartition != null) {
                            memberApiBean.setPartitionName(tapePartition.getName());
                        }
                    } else if (member.getPoolPartitionId() != null) {
                        final PoolPartition poolPartition = serviceManager.getRetriever(PoolPartition.class).retrieve(member.getPoolPartitionId());
                        if (poolPartition != null) {
                            memberApiBean.setPartitionName(poolPartition.getName());
                        }
                    }

                    memberApiBeans.add(memberApiBean);
                }
                storageDomainApiBean.setStorageDomainMembers(CollectionFactory.toArray(StorageDomainMemberApiBean.class, memberApiBeans));
            } else {
                storageDomainApiBean.setStorageDomainMembers(null);
            }

            storageDomainApiBeans.add(storageDomainApiBean);
        }

        return storageDomainApiBeans.isEmpty() ? null : CollectionFactory.toArray(StorageDomainApiBean.class, storageDomainApiBeans);
    }

    private TapePartitionApiBean[] populateTapePartitions(final BeansServiceManager serviceManager) {
        final List<TapePartitionApiBean> tapePartitionApiBeans = new ArrayList<>();
        final List<TapePartition> allTapePartitions = serviceManager.getRetriever(TapePartition.class).retrieveAll().toList();

        for (final TapePartition tapePartition : allTapePartitions) {
            final TapePartitionRM tapePartitionRM = new TapePartitionRM(tapePartition, serviceManager);

            final TapePartitionApiBean tapePartitionApiBean = BeanFactory.newBean(TapePartitionApiBean.class);
            BeanCopier.copy(tapePartitionApiBean, tapePartition);

            // Set tape and drive counts
            tapePartitionApiBean.setTapeCount(tapePartitionRM.getTapes().toList().size());
            tapePartitionApiBean.setDriveCount(tapePartitionRM.getTapeDrives().toList().size());

            tapePartitionApiBeans.add(tapePartitionApiBean);
        }

        return tapePartitionApiBeans.isEmpty() ? null : CollectionFactory.toArray(TapePartitionApiBean.class, tapePartitionApiBeans);
    }

    private PoolPartitionApiBean[] populatePoolPartitions(final BeansServiceManager serviceManager) {
        final List<PoolPartitionApiBean> poolPartitionApiBeans = new ArrayList<>();
        final List<PoolPartition> allPoolPartitions = serviceManager.getRetriever(PoolPartition.class).retrieveAll().toList();

        for (final PoolPartition poolPartition : allPoolPartitions) {
            final PoolPartitionRM poolPartitionRM = new PoolPartitionRM(poolPartition, serviceManager);

            final PoolPartitionApiBean poolPartitionApiBean = BeanFactory.newBean(PoolPartitionApiBean.class);
            BeanCopier.copy(poolPartitionApiBean, poolPartition);

            // Set pool count
            poolPartitionApiBean.setPoolCount(poolPartitionRM.getPools().toList().size());

            poolPartitionApiBeans.add(poolPartitionApiBean);
        }

        return poolPartitionApiBeans.isEmpty() ? null : CollectionFactory.toArray(PoolPartitionApiBean.class, poolPartitionApiBeans);
    }

    private TargetApiBean[] populateTargets(final BeansServiceManager serviceManager) {
        final List<TargetApiBean> targetApiBeans = new ArrayList<>();

        // Add DS3 targets (no naming mode)
        final List<Ds3Target> ds3Targets = serviceManager.getRetriever(Ds3Target.class).retrieveAll().toList();
        for (final Ds3Target ds3Target : ds3Targets) {
            final TargetApiBean targetApiBean = BeanFactory.newBean(TargetApiBean.class);
            BeanCopier.copy(targetApiBean, ds3Target);
            targetApiBeans.add(targetApiBean);
        }

        // Add S3 targets (with naming mode)
        final List<S3Target> s3Targets = serviceManager.getRetriever(S3Target.class).retrieveAll().toList();
        for (final S3Target s3Target : s3Targets) {
            final TargetApiBean targetApiBean = BeanFactory.newBean(TargetApiBean.class);
            BeanCopier.copy(targetApiBean, s3Target);
            targetApiBean.setCloudNamingMode(s3Target.getNamingMode());
            targetApiBeans.add(targetApiBean);
        }

        // Add Azure targets (with naming mode)
        final List<AzureTarget> azureTargets = serviceManager.getRetriever(AzureTarget.class).retrieveAll().toList();
        for (final AzureTarget azureTarget : azureTargets) {
            final TargetApiBean targetApiBean = BeanFactory.newBean(TargetApiBean.class);
            BeanCopier.copy(targetApiBean, azureTarget);
            targetApiBean.setCloudNamingMode(azureTarget.getNamingMode());
            targetApiBeans.add(targetApiBean);
        }

        return targetApiBeans.isEmpty() ? null : CollectionFactory.toArray(TargetApiBean.class, targetApiBeans);
    }
}
