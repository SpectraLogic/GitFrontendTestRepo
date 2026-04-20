package com.spectralogic.integrations;

public final class TestConstants {
    public final static String AZURE_ACCOUNT_NAME = "devstoreaccount1";//
    public final static String AZURE_ACCOUNT_KEY =
            "Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==";

    public final static String DATA_POLICY_S3_SINGLE_COPY_NAME = "Single Copy on S3";
    public final static String DATA_POLICY_AZURE_SINGLE_COPY_NAME = "Single Copy on Azure";
    public final static String DATA_POLICY_TAPE_SINGLE_COPY_NAME = "Single Copy on Tape";
    public final static String DATA_POLICY_VERSIONS = "Data Policy Versions";
    public final static String DATA_POLICY_TAPE_REPLICATION_COPY_NAME = "Single Copy on Tape and BP";
    public final static String DATA_POLICY_TAPE_DUAL_COPY_NAME = "Dual Copy on Tape";
    public final static String DATA_POLICY_POOL_NAME = "Single Copy on Nearline Disk";
    public final static String STORAGE_DOMAIN_POOL_NAME = "Pool First Copy";
    public final static String STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME = "Tape First Copy";
    public final static String STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME = "Tape Second Copy";
    public final static String DATA_POLICY_HYBRID_DUAL_COPY_NAME =
            "Single Copy on Nearline Disk and Dual Copy on Tape";
    public final static String DATA_POLICY_HYBRID_SINGLE_COPY_NAME = "Single Copy on Nearline Disk and Tape";
    public final static String authId = "Administrator_authid";
    public final static String secretKey = "mySecretKey";
    final static String ds3AccessKey = System.getenv("DS3_ACCESS_KEY");
    final static String ds3SecretKey = System.getenv("DS3_SECRET_KEY");
    final static String BP_USED = System.getenv("BP_USED");

     public final static  String AZURITE_CONNECTION_STRING =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=devstoreaccount1;" +
                    "AccountKey=Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==;" +
                    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

}
