package com.spectralogic.integrations;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.spectralogic.integrations.DatabaseUtils.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.getAllDataPolicies;
import static com.spectralogic.integrations.Ds3ApiHelpers.updateGroupMember;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestUtils.*;

public class Ds3ReplicationUtils {

    public static void clearPersistenceRules(Ds3Client client) throws IOException {
        GetDataPersistenceRulesSpectraS3Request getDataPersistenceRulesSpectraS3Request = new GetDataPersistenceRulesSpectraS3Request();
        GetDataPersistenceRulesSpectraS3Response allResponses = client.getDataPersistenceRulesSpectraS3(getDataPersistenceRulesSpectraS3Request);

        List<DataPersistenceRule> rules = allResponses.getDataPersistenceRuleListResult().getDataPersistenceRules();

        for (DataPersistenceRule rule : rules) {
            DeleteDataPersistenceRuleSpectraS3Request deleteRule = new DeleteDataPersistenceRuleSpectraS3Request(rule.getId());
            client.deleteDataPersistenceRuleSpectraS3(deleteRule);
        }

    }

    public static void clearDs3ReplicationRules(Ds3Client client) throws IOException {
        GetDs3DataReplicationRulesSpectraS3Request getDs3DataReplicationRulesSpectraS3Request = new GetDs3DataReplicationRulesSpectraS3Request();
        GetDs3DataReplicationRulesSpectraS3Response allDs3RulesResponse = client.getDs3DataReplicationRulesSpectraS3(getDs3DataReplicationRulesSpectraS3Request);
        List<DataPolicy> policies = getAllDataPolicies(client);

        for (DataPolicy policy : policies) {
            for (Ds3DataReplicationRule rule :allDs3RulesResponse.getDs3DataReplicationRuleListResult().getDs3DataReplicationRules()) {
                if (rule.getDataPolicyId().equals(policy.getId()) ) {
                    DeleteDs3DataReplicationRuleSpectraS3Request deleteD3Data = new DeleteDs3DataReplicationRuleSpectraS3Request(rule.getId().toString());
                    client.deleteDs3DataReplicationRuleSpectraS3(deleteD3Data);
                }
            }
        }
    }

    public static void clearDs3ReplicationRules(Ds3Client client, String dataPolicyName) throws IOException {
        GetDs3DataReplicationRulesSpectraS3Request getDs3DataReplicationRulesSpectraS3Request = new GetDs3DataReplicationRulesSpectraS3Request();
        GetDs3DataReplicationRulesSpectraS3Response allDs3RulesResponse = client.getDs3DataReplicationRulesSpectraS3(getDs3DataReplicationRulesSpectraS3Request);
        List<DataPolicy> policies = getAllDataPolicies(client);
        Optional<DataPolicy> foundPolicy = policies.stream()
                .filter(model -> model.getName().equals(dataPolicyName)).findFirst();
        for (Ds3DataReplicationRule rule :allDs3RulesResponse.getDs3DataReplicationRuleListResult().getDs3DataReplicationRules()) {
            if (rule.getDataPolicyId().equals(foundPolicy.get().getId()) ) {
                DeleteDs3DataReplicationRuleSpectraS3Request deleteDs3Data = new DeleteDs3DataReplicationRuleSpectraS3Request(rule.getId().toString());
                client.deleteDs3DataReplicationRuleSpectraS3(deleteDs3Data);
            }
        }
    }

    public static void clearS3ReplicationRules(Ds3Client client, String dataPolicyName) throws IOException {
        GetS3DataReplicationRulesSpectraS3Request getS3DataReplicationRulesSpectraS3Request = new GetS3DataReplicationRulesSpectraS3Request();
        GetS3DataReplicationRulesSpectraS3Response allS3RulesResponse = client.getS3DataReplicationRulesSpectraS3(getS3DataReplicationRulesSpectraS3Request);
        List<DataPolicy> policies = getAllDataPolicies(client);
        Optional<DataPolicy> foundPolicy = policies.stream()
                .filter(model -> model.getName().equals(dataPolicyName)).findFirst();
        for (S3DataReplicationRule rule :allS3RulesResponse.getS3DataReplicationRuleListResult().getS3DataReplicationRules()) {
            if (rule.getDataPolicyId().equals(foundPolicy.get().getId()) ) {
                DeleteS3DataReplicationRuleSpectraS3Request deleteS3Data = new DeleteS3DataReplicationRuleSpectraS3Request(rule.getId().toString());
                client.deleteS3DataReplicationRuleSpectraS3(deleteS3Data);
            }
        }
    }

    public static void clearS3ReplicationRules(Ds3Client client) throws IOException {
        GetS3DataReplicationRulesSpectraS3Request getS3DataReplicationRulesSpectraS3Request = new GetS3DataReplicationRulesSpectraS3Request();
        GetS3DataReplicationRulesSpectraS3Response allS3RulesResponse = client.getS3DataReplicationRulesSpectraS3(getS3DataReplicationRulesSpectraS3Request);
        List<DataPolicy> policies = getAllDataPolicies(client);

        for (DataPolicy policy : policies) {
            for (S3DataReplicationRule rule :allS3RulesResponse.getS3DataReplicationRuleListResult().getS3DataReplicationRules()) {
                if (rule.getDataPolicyId().equals(policy.getId()) ) {
                    DeleteS3DataReplicationRuleSpectraS3Request deleteS3Data = new DeleteS3DataReplicationRuleSpectraS3Request(rule.getId().toString());
                    client.deleteS3DataReplicationRuleSpectraS3(deleteS3Data);
                }
            }
        }
    }

    public static void clearAzureReplicationRules(Ds3Client client) throws IOException {
        GetAzureDataReplicationRulesSpectraS3Request getAzureDataReplicationRulesSpectraS3Request = new GetAzureDataReplicationRulesSpectraS3Request();
        GetAzureDataReplicationRulesSpectraS3Response allAzureRulesResponse = client.getAzureDataReplicationRulesSpectraS3(getAzureDataReplicationRulesSpectraS3Request);

        List<DataPolicy> policies = getAllDataPolicies(client);

        for (DataPolicy policy : policies) {
            for (AzureDataReplicationRule rule :allAzureRulesResponse.getAzureDataReplicationRuleListResult().getAzureDataReplicationRules()) {
                if (rule.getDataPolicyId().equals(policy.getId()) ) {
                    DeleteAzureDataReplicationRuleSpectraS3Request deleteAzureData = new DeleteAzureDataReplicationRuleSpectraS3Request(rule.getId().toString());
                    client.deleteAzureDataReplicationRuleSpectraS3(deleteAzureData);
                }
            }
        }

    }
    public static void clearAzureReplicationRules(Ds3Client client, String dataPolicyName) throws IOException {
        List<DataPolicy> policies = getAllDataPolicies(client);
        List<DeleteAzureDataReplicationRuleSpectraS3Request> deleteAzureDataRequests = new ArrayList<>();
        for (DataPolicy policy : policies) {
            GetAzureDataReplicationRulesSpectraS3Request getAzureDataReplicationRulesSpectraS3Request = new GetAzureDataReplicationRulesSpectraS3Request();
            GetAzureDataReplicationRulesSpectraS3Response allAzureRulesResponse = client.getAzureDataReplicationRulesSpectraS3(getAzureDataReplicationRulesSpectraS3Request);
            for (AzureDataReplicationRule rule : allAzureRulesResponse.getAzureDataReplicationRuleListResult().getAzureDataReplicationRules()) {
                if (rule.getDataPolicyId().equals(policy.getId())) {
                    DeleteAzureDataReplicationRuleSpectraS3Request deleteAzureData = new DeleteAzureDataReplicationRuleSpectraS3Request(rule.getId().toString());
                    deleteAzureDataRequests.add(deleteAzureData);
                }
            }
        }

        for (DeleteAzureDataReplicationRuleSpectraS3Request request : deleteAzureDataRequests) {
            try {
                DeleteAzureDataReplicationRuleSpectraS3Response response = client.deleteAzureDataReplicationRuleSpectraS3(request);
            } catch (Exception e) {
                System.out.println("Delete Azure re" + e);
            }

        }
    }

    public static void clearDs3Targets(Ds3Client client) throws IOException {
        GetDs3TargetsSpectraS3Request request = new GetDs3TargetsSpectraS3Request();
        GetDs3TargetsSpectraS3Response response = client.getDs3TargetsSpectraS3(request);
        List<Ds3Target> targets = response.getDs3TargetListResult().getDs3Targets();
        for (Ds3Target t: targets) {
            DeleteDs3TargetSpectraS3Request deleteDs3TargetSpectraS3Request = new DeleteDs3TargetSpectraS3Request(t.getName());
            client.deleteDs3TargetSpectraS3(deleteDs3TargetSpectraS3Request);
        }
    }
    public static void clearS3Targets(Ds3Client client) throws IOException {
        GetS3TargetsSpectraS3Request request = new GetS3TargetsSpectraS3Request();
        GetS3TargetsSpectraS3Response response = client.getS3TargetsSpectraS3(request);
        List<S3Target> targets = response.getS3TargetListResult().getS3Targets();
        for (S3Target t: targets) {
            DeleteS3TargetSpectraS3Request deleteS3TargetSpectraS3Request = new DeleteS3TargetSpectraS3Request(t.getName());
            client.deleteS3TargetSpectraS3(deleteS3TargetSpectraS3Request);
        }
    }

    public static void clearAzureTargets(Ds3Client client) throws IOException {
        GetAzureTargetsSpectraS3Request request = new GetAzureTargetsSpectraS3Request();
        GetAzureTargetsSpectraS3Response response = client.getAzureTargetsSpectraS3(request);
        List<AzureTarget> targets = response.getAzureTargetListResult().getAzureTargets();
        for (AzureTarget t: targets) {
            DeleteAzureTargetSpectraS3Request deleteAzureTargetSpectraS3Request = new DeleteAzureTargetSpectraS3Request(t.getName());
            client.deleteAzureTargetSpectraS3(deleteAzureTargetSpectraS3Request);
        }
    }



    public static Ds3Target registerDockerDs3Target(Ds3Client client, String authId, String secretKey, TargetReadPreferenceType targetReadPreferenceType) throws IOException, SQLException {
        UUID userId = getDockerUserId();
        UUID groupId = getGroupId();
        updateGroupMember(groupId, userId);

        updateRemoteUser();

        RegisterDs3TargetSpectraS3Request registerDs3TargetSpectraS3Request = new RegisterDs3TargetSpectraS3Request
                ( authId, secretKey, "tomcatreplica:8080", "BP-Docker");

        registerDs3TargetSpectraS3Request.withDataPathHttps(false);
        registerDs3TargetSpectraS3Request.withDataPathVerifyCertificate(false);
        if (targetReadPreferenceType != null) {
            registerDs3TargetSpectraS3Request.withDefaultReadPreference(targetReadPreferenceType);
        }

        RegisterDs3TargetSpectraS3Response resp = client.registerDs3TargetSpectraS3(registerDs3TargetSpectraS3Request);
        return resp.getDs3TargetResult();
    }

    //sm2u-1.eng.sldomain.com

    public static S3Target registerS3VailTarget(Ds3Client client) throws IOException {
        RegisterS3TargetSpectraS3Request registerS3TargetSpectraS3Request = new RegisterS3TargetSpectraS3Request
                ( "SKISGPR2RUYOO9FGVSNY", "Vailtarget", "Kh5mMm11THMfNKDTxgOQamC3D5wX549jlb6snlp8");
        registerS3TargetSpectraS3Request.withDataPathEndPoint("sm2u-1.eng.sldomain.com:8080");
        registerS3TargetSpectraS3Request.withRegion(S3Region.US_EAST_1);
        registerS3TargetSpectraS3Request.withHttps(false);

        RegisterS3TargetSpectraS3Response resp = client.registerS3TargetSpectraS3(registerS3TargetSpectraS3Request);
        return resp.getS3TargetResult();
    }

    public static S3Target registerS3LocalstackTarget(Ds3Client client) throws IOException {
        if ( BP_USED != null && BP_USED.equals("true")) {
            return registerS3VailTarget(client);
        } else {
            RegisterS3TargetSpectraS3Request registerS3TargetSpectraS3Request = new RegisterS3TargetSpectraS3Request
                    ( "test", "s3target", "S3Test");
            registerS3TargetSpectraS3Request.withDataPathEndPoint("s3.localstack:4566");
            registerS3TargetSpectraS3Request.withRegion(S3Region.US_EAST_1);
            registerS3TargetSpectraS3Request.withHttps(false);

            RegisterS3TargetSpectraS3Response resp = client.registerS3TargetSpectraS3(registerS3TargetSpectraS3Request);
            return resp.getS3TargetResult();
        }

    }

    public static S3Target registerS3NativeLocalstackTarget(Ds3Client client) throws IOException {
        if ( BP_USED != null && BP_USED.equals("true")) {
            return registerS3VailTarget(client);
        } else {
            RegisterS3TargetSpectraS3Request registerS3TargetSpectraS3Request = new RegisterS3TargetSpectraS3Request
                    ( "test", "s3target", "S3Test");
            registerS3TargetSpectraS3Request.withDataPathEndPoint("s3.localstack:4566");
            registerS3TargetSpectraS3Request.withRegion(S3Region.US_EAST_1);
            registerS3TargetSpectraS3Request.withHttps(false);
            registerS3TargetSpectraS3Request.withNamingMode(CloudNamingMode.AWS_S3);

            RegisterS3TargetSpectraS3Response resp = client.registerS3TargetSpectraS3(registerS3TargetSpectraS3Request);
            return resp.getS3TargetResult();
        }

    }





    public static AzureTarget registerAzuriteTarget(Ds3Client client) throws IOException {
        RegisterAzureTargetSpectraS3Request registerAzureTargetSpectraS3Request = new RegisterAzureTargetSpectraS3Request
                ( AZURE_ACCOUNT_KEY, AZURE_ACCOUNT_NAME, "AzureTest");
        registerAzureTargetSpectraS3Request.withHttps(false);

        RegisterAzureTargetSpectraS3Response resp = client.registerAzureTargetSpectraS3(registerAzureTargetSpectraS3Request);
        return resp.getAzureTargetResult();
    }

    public static Ds3Target registerDs3Target(Ds3Client client, String authId, String secretKey) throws IOException, SQLException {
        RegisterDs3TargetSpectraS3Request registerDs3TargetSpectraS3Request = new RegisterDs3TargetSpectraS3Request
                ( authId, secretKey, "BP1.dyn.eng.sldomain.com", "BPsm4u-4");

        registerDs3TargetSpectraS3Request.withDataPathHttps(false);
        RegisterDs3TargetSpectraS3Response resp = client.registerDs3TargetSpectraS3(registerDs3TargetSpectraS3Request);
        return resp.getDs3TargetResult();
    }
}
