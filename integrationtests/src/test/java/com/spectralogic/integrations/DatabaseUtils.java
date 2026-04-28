package com.spectralogic.integrations;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.spectralogic.integrations.TestConstants.*;


public class DatabaseUtils {

    public static Connection getTestDatabaseConnection() {
        Connection connection;
        try{
            connection =  DriverManager.getConnection("jdbc:postgresql://localhost:5431/tapesystem", "Administrator", "dogfish");
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void quiescePartition() throws SQLException {
        String updateSQL = "UPDATE tape.tape_partition SET quiesced = ?::shared.quiesced ";
        Connection connection = null;
        try{
            connection= getTestDatabaseConnection();
            PreparedStatement statement = connection.prepareStatement(updateSQL);

            statement.setString(1, "NO");

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Updating tape partition failed, no rows affected.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update pool partition via JDBC", e);
        }
        finally {
            assert connection != null;
            connection.close();
        }
    }

    public static void quiesceTape() throws SQLException {
       // String updateSQL = "UPDATE tape.tape SET state = ?::tape.tape_state";
        String updateSQL = "UPDATE tape.tape_partition SET state = ?::shared.quiesced";
        Connection connection = null;
        try{
            connection= getTestDatabaseConnection();
            PreparedStatement statement = connection.prepareStatement(updateSQL);

            statement.setString(1, "YES");

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Updating pool partition failed, no rows affected.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update pool partition via JDBC", e);
        }
        finally {
            assert connection != null;
            connection.close();
        }
    }


    public static void updatePoolPartition(String guid, String partitionId) throws SQLException {
        String updateSQL = "UPDATE pool.pool SET partition_id = ? WHERE guid = ?";
        Connection connection = null;
        try{
            connection= getTestDatabaseConnection();
            PreparedStatement statement = connection.prepareStatement(updateSQL);
            UUID uuid = UUID.fromString(partitionId);
            statement.setObject(1, uuid);
            statement.setString(2, guid);
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Updating pool partition failed, no rows affected.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update pool partition via JDBC", e);
        }
        finally {
            assert connection != null;
            connection.close();
        }
    }

    public static UUID getDockerUserId() {
        try{
            Connection connection = getTestDatabaseConnection();
            String getUserSql = "SELECT id FROM ds3.user WHERE name = ?";
            PreparedStatement selectStatement = connection.prepareStatement(getUserSql);
            selectStatement.setString(1, "Administrator");
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                if (resultSet.next()) {
                    return  (UUID) resultSet.getObject("id");
                }
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void addGroupMember(UUID groupId, UUID memberId) throws SQLException {
        try{
            Connection connection = getTestDatabaseConnection();
            String insertSQL = "INSERT INTO ds3.group_member (group_id, id, member_user_id ) VALUES (?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(insertSQL);
            statement.setObject(1, groupId);
            statement.setObject(2, UUID.randomUUID());
            statement.setObject(3, memberId);
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Adding group member failed, no rows affected.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to add  group member via JDBC", e);
        }
    }

    public static void createUser(String authId, String secretKey) throws SQLException {
        String AdminId = "";
        String insertSQL = "INSERT INTO ds3.user (auth_id, id, max_buckets, name, secret_key) VALUES (?, ?, ?, ?, ?)";
        try{
            Connection connection = getTestDatabaseConnection();

            String selectSQL = "SELECT 1 FROM ds3.user WHERE auth_id = ?";
            try (PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
                selectStatement.setString(1, authId);

                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    if (resultSet.next()) {
                        System.out.println("User with Auth ID " + authId + " already exists. Skipping insertion.");
                        // Exit the method if the user is found
                        return;
                    }
                }
            }
            UUID userId = UUID.randomUUID();
            PreparedStatement statement = connection.prepareStatement(insertSQL);
            statement.setString(1, authId);
            statement.setObject(2, userId);
            statement.setInt(3, 1000);
            statement.setString(4, "ReplicationUser");
            statement.setString(5, secretKey);

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Creating user failed, no rows affected.");
            }
            UUID groupId = getGroupId();
            addGroupMember(groupId, userId);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create user via JDBC", e);
        }
    }

    public static void updateTapesStatus() throws SQLException {
        String updateSQL = "UPDATE tape.tape SET state = ?::tape.tape_state";
        try{
            Connection connection = getTestDatabaseConnection();
            PreparedStatement statement = connection.prepareStatement(updateSQL);
            statement.setString(1, TapeState.NORMAL.name());

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Updating tape status failed, no rows affected.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update tape status via JDBC", e);
        }
    }

    public static UUID getGroupId() {
        try{
            Connection connection = getTestDatabaseConnection();
            String selectGroupSQL = "SELECT id FROM ds3.group WHERE name = ?";
            PreparedStatement selectStatement = connection.prepareStatement(selectGroupSQL);
            selectStatement.setString(1, "Administrators");

            try (ResultSet resultSet = selectStatement.executeQuery()) {
                if (resultSet.next()) {
                    return (UUID) resultSet.getObject("id");

                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch admin group ID via JDBC", e);
        }
    }

    public static void checkAndUpdateGroup(UUID groupId, UUID userId, Connection connection) throws SQLException {
        String selectSQL = "SELECT * FROM ds3.group_member WHERE group_id = ? AND member_user_id = ?";
        PreparedStatement selectStatement = connection.prepareStatement(selectSQL);
        selectStatement.setObject(1, groupId);
        selectStatement.setObject(2, userId);
        try (ResultSet resultGroupSet = selectStatement.executeQuery()) {
            if (resultGroupSet.next()) {
                return;
            }
            String insertSQL = "INSERT INTO ds3.group_member (group_id, id, member_user_id ) VALUES (?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(insertSQL);
            statement.setObject(1, groupId);
            statement.setObject(2, UUID.randomUUID());
            statement.setObject(3, userId);
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Adding group member failed, no rows affected.");
            }
        }
    }

    public static void markBlobSuspect()  {
        final String selectSQL = "SELECT blob_id, order_index, tape_id, id FROM tape.blob_tape";
        final String insertSQL = "INSERT INTO tape.suspect_blob_tape (blob_id, id, order_index, tape_id) VALUES (?, ?, ?, ?)";

        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL);
             final ResultSet resultSet = selectStatement.executeQuery();
             final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

            while (resultSet.next()) {
                insertStatement.setObject(1, resultSet.getObject("blob_id"));
                insertStatement.setObject(2, resultSet.getObject("id"));
                insertStatement.setObject(3, resultSet.getObject("order_index"));
                insertStatement.setObject(4, resultSet.getObject("tape_id"));
                final int rowsAffected = insertStatement.executeUpdate();
                if (rowsAffected == 0) {
                    throw new SQLException("Marking blob as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect via JDBC", e);
        }

    }

    public static void markBlobSuspectForTape(final UUID tapeId)  {
        final String selectSQL = "SELECT blob_id, order_index, tape_id, id FROM tape.blob_tape WHERE tape_id = ?";
        final String insertSQL = "INSERT INTO tape.suspect_blob_tape (blob_id, id, order_index, tape_id) VALUES (?, ?, ?, ?)";

        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {

            selectStatement.setObject(1, tapeId);

            try (final ResultSet resultSet = selectStatement.executeQuery();
                 final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    insertStatement.setObject(1, resultSet.getObject("blob_id"));
                    insertStatement.setObject(2, resultSet.getObject("id"));
                    insertStatement.setObject(3, resultSet.getObject("order_index"));
                    insertStatement.setObject(4, resultSet.getObject("tape_id"));
                    final int rowsAffected = insertStatement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Marking blob as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect for tape " + tapeId + " via JDBC", e);
        }

    }

    public static void markBlobSuspectForTape(final UUID tapeId, UUID blobId)  {
        final String selectSQL = "SELECT blob_id, order_index, tape_id, id FROM tape.blob_tape WHERE  tape_id = ? AND blob_id = ?";
        final String insertSQL = "INSERT INTO tape.suspect_blob_tape (blob_id, id, order_index, tape_id) VALUES (?, ?, ?, ?)";
        int count = 0;
        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {

            selectStatement.setObject(1, tapeId);
            selectStatement.setObject(2, blobId);

            try (final ResultSet resultSet = selectStatement.executeQuery();
                 final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    insertStatement.setObject(1, resultSet.getObject("blob_id"));
                    insertStatement.setObject(2, resultSet.getObject("id"));
                    insertStatement.setObject(3, resultSet.getObject("order_index"));
                    insertStatement.setObject(4, resultSet.getObject("tape_id"));
                    final int rowsAffected = insertStatement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Marking blob as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                    }
                    count++;

                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect for tape " + tapeId + " via JDBC", e);
        }

    }


    public static void markBlobSuspectForTape(final UUID tapeId, int recordCount)  {
        final String selectSQL = "SELECT blob_id, order_index, tape_id, id FROM tape.blob_tape WHERE tape_id = ?";
        final String insertSQL = "INSERT INTO tape.suspect_blob_tape (blob_id, id, order_index, tape_id) VALUES (?, ?, ?, ?)";
        int count = 0;
        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {

            selectStatement.setObject(1, tapeId);

            try (final ResultSet resultSet = selectStatement.executeQuery();
                 final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    insertStatement.setObject(1, resultSet.getObject("blob_id"));
                    insertStatement.setObject(2, resultSet.getObject("id"));
                    insertStatement.setObject(3, resultSet.getObject("order_index"));
                    insertStatement.setObject(4, resultSet.getObject("tape_id"));
                    final int rowsAffected = insertStatement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Marking blob as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                    }
                    count++;
                    if (count >= recordCount) {
                        return;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect for tape " + tapeId + " via JDBC", e);
        }

    }

    public static UUID getS3SuspectBlob() {
        try{
            Connection connection = getTestDatabaseConnection();
            String selectGroupSQL = "SELECT * FROM target.suspect_blob_s3_target";
            PreparedStatement selectStatement = connection.prepareStatement(selectGroupSQL);


            try (ResultSet resultSet = selectStatement.executeQuery()) {
                if (resultSet.next()) {
                    return (UUID) resultSet.getObject("id");

                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch suspect blob", e);
        }
    }

    public static void corruptCheckSum() {
        final String selectSQL = "SELECT id, checksum FROM ds3.blob WHERE checksum IS NOT NULL LIMIT 1";
        final String updateSQL = "UPDATE ds3.blob SET checksum = ? WHERE id = ?";

        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL);
             final ResultSet resultSet = selectStatement.executeQuery()) {

            if (resultSet.next()) {
                final UUID blobId = (UUID) resultSet.getObject("id");
                final String originalChecksum = resultSet.getString("checksum");

                if (originalChecksum == null || originalChecksum.length() < 2) {
                    // Not a valid string to swap, so we can't corrupt it.
                    return;
                }

                // Swap the first and last characters
                final char firstChar = originalChecksum.charAt(0);
                final char lastChar = originalChecksum.charAt(originalChecksum.length() - 1);
                final String middle = originalChecksum.substring(1, originalChecksum.length() - 1);
                final String swappedChecksum = lastChar + middle + firstChar;

                try (final PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
                    updateStatement.setString(1, swappedChecksum);
                    updateStatement.setObject(2, blobId);
                    updateStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to corrupt checksum", e);
        }

    }

    public static void markAzureBlobSuspect()  {
        markAzureBlobSuspect(Integer.MAX_VALUE);
    }

    public static void markAzureBlobSuspect(int recordCount)  {
        final String selectSQL = "SELECT blob_id, id, target_id FROM target.blob_azure_target LIMIT ?";
        final String insertSQL = "INSERT INTO target.suspect_blob_azure_target (blob_id, id, target_id) VALUES (?, ?, ?)";

        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
            selectStatement.setInt(1, recordCount);
            try (final ResultSet resultSet = selectStatement.executeQuery();
                 final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    insertStatement.setObject(1, resultSet.getObject("blob_id"));
                    insertStatement.setObject(2, resultSet.getObject("id"));
                    insertStatement.setObject(3, resultSet.getObject("target_id"));
                    final int rowsAffected = insertStatement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Marking blob for axure as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect for Azure svia JDBC", e);
        }

    }

    public static void deleteObjectsFromBucket(String bucketName, int count) {
        final String selectSQL =
                "SELECT o.id AS object_id FROM ds3.s3_object o " +
                "JOIN ds3.bucket b ON o.bucket_id = b.id " +
                "WHERE b.name = ? LIMIT ?";
        final String deleteBlobTapeSQL = "DELETE FROM tape.blob_tape WHERE blob_id IN (SELECT id FROM ds3.blob WHERE object_id = ?)";
        final String deleteBlobSQL = "DELETE FROM ds3.blob WHERE object_id = ?";
        final String deleteObjectSQL = "DELETE FROM ds3.s3_object WHERE id = ?";

        try (final Connection connection = getTestDatabaseConnection()) {
            connection.setAutoCommit(false);

            final List<Object> objectIds = new ArrayList<>();
            try (final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
                selectStatement.setString(1, bucketName);
                selectStatement.setInt(2, count);
                try (final ResultSet resultSet = selectStatement.executeQuery()) {
                    while (resultSet.next()) {
                        objectIds.add(resultSet.getObject("object_id"));
                    }
                }
            }

            try (final PreparedStatement deleteBlobTape = connection.prepareStatement(deleteBlobTapeSQL);
                 final PreparedStatement deleteBlob = connection.prepareStatement(deleteBlobSQL);
                 final PreparedStatement deleteObject = connection.prepareStatement(deleteObjectSQL)) {
                for (Object objectId : objectIds) {
                    deleteBlobTape.setObject(1, objectId);
                    deleteBlobTape.executeUpdate();
                    deleteBlob.setObject(1, objectId);
                    deleteBlob.executeUpdate();
                    deleteObject.setObject(1, objectId);
                    deleteObject.executeUpdate();
                }
            }

            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete objects from database for bucket " + bucketName, e);
        }
    }

    private static String savedUnavailableMediaPolicy;

    public static void setUnavailableMediaPolicyDiscouraged() {
        final String selectSQL = "SELECT unavailable_media_policy FROM ds3.data_path_backend";
        final String updateSQL = "UPDATE ds3.data_path_backend SET unavailable_media_policy = ?::ds3.unavailable_media_usage_policy";

        try (final Connection connection = getTestDatabaseConnection()) {
            try (final PreparedStatement selectStatement = connection.prepareStatement(selectSQL);
                 final ResultSet resultSet = selectStatement.executeQuery()) {
                if (resultSet.next()) {
                    savedUnavailableMediaPolicy = resultSet.getString(1);
                }
            }
            try (final PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
                updateStatement.setString(1, "DISCOURAGED");
                final int rowsAffected = updateStatement.executeUpdate();
                if (rowsAffected == 0) {
                    throw new SQLException("Updating unavailable_media_policy failed, no rows affected.");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set unavailable_media_policy to DISCOURAGED", e);
        }
    }

    public static void resetUnavailableMediaPolicy() {
        if (savedUnavailableMediaPolicy == null) {
            return;
        }
        final String updateSQL = "UPDATE ds3.data_path_backend SET unavailable_media_policy = ?::ds3.unavailable_media_usage_policy";
        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement statement = connection.prepareStatement(updateSQL)) {
            statement.setString(1, savedUnavailableMediaPolicy);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to reset unavailable_media_policy", e);
        } finally {
            savedUnavailableMediaPolicy = null;
        }
    }

    public static boolean isBlobCacheEmpty() {
        final String countSQL = "SELECT COUNT(*) FROM planner.blob_cache";
        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement statement = connection.prepareStatement(countSQL);
             final ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1) == 0;
            }
            return true;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query planner.blob_cache via JDBC", e);
        }
    }

    public static void markS3BlobSuspect()  {
        markS3BlobSuspect(Integer.MAX_VALUE);
    }

    public static void markS3BlobSuspect(int recordCount)  {
        final String selectSQL = "SELECT blob_id, id, target_id FROM target.blob_s3_target LIMIT ?";
        final String insertSQL = "INSERT INTO target.suspect_blob_s3_target (blob_id, id, target_id) VALUES (?, ?, ?)";

        try (final Connection connection = getTestDatabaseConnection();
             final PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
            selectStatement.setInt(1, recordCount);
            try (final ResultSet resultSet = selectStatement.executeQuery();
                 final PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    insertStatement.setObject(1, resultSet.getObject("blob_id"));
                    insertStatement.setObject(2, resultSet.getObject("id"));
                    insertStatement.setObject(3, resultSet.getObject("target_id"));
                    final int rowsAffected = insertStatement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Marking blob for S3 as suspect failed, no rows affected for blob_id: " + resultSet.getObject("blob_id"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob(s) as suspect for S3 via JDBC", e);
        }

    }

    public static Connection getRemoteTestDatabaseConnection() {
        Connection connection;
        try{
            connection =  DriverManager.getConnection("jdbc:postgresql://localhost:5533/tapesystem", "Administrator", "dogfish");
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateRemoteUser() {
        try{
            Connection connection = getRemoteTestDatabaseConnection();
            String getUserSql = "SELECT id FROM ds3.user WHERE name = ?";
            PreparedStatement selectUserStatement = connection.prepareStatement(getUserSql);
            selectUserStatement.setString(1, "Administrator");
            try (ResultSet resultSet = selectUserStatement.executeQuery()) {
                if (resultSet.next()) {
                    UUID userId =  (UUID) resultSet.getObject("id");
                    String selectGroupSQL = "SELECT id FROM ds3.group WHERE name = ?";
                    PreparedStatement selectGroupStatement = connection.prepareStatement(selectGroupSQL);
                    selectGroupStatement.setString(1, "Administrators");

                    try (ResultSet resultGroupSet = selectGroupStatement.executeQuery()) {
                        if (resultGroupSet.next()) {
                            UUID groupId =  (UUID) resultGroupSet.getObject("id");
                            checkAndUpdateGroup(groupId, userId, connection);
                        }

                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void revertBlobSize(String dataPolicyName) throws SQLException {
        String sql = "UPDATE ds3.data_policy SET default_blob_size = ?, blobbing_enabled = ? WHERE name = ?";

        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getTestDatabaseConnection(); // Placeholder for your connection logic
            statement = connection.prepareStatement(sql);
            statement.setNull(1, java.sql.Types.BIGINT);
            statement.setBoolean(2, false);
            statement.setString(3, dataPolicyName);
            int rowsAffected = statement.executeUpdate();

            if (rowsAffected == 0) {
                throw new SQLException("Error updating default blob size.");
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to update default blob size", e);
        } finally {
            assert connection != null;
            connection.close();
        }
    }

    public static void removeBlobs (int count) {
        String sql = "DELETE FROM tape.blob_tape " +
                "WHERE id IN (SELECT id FROM tape.blob_tape LIMIT ?)";

        try (Connection connection = getTestDatabaseConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setInt(1, count);
            int rowsAffected = statement.executeUpdate();

            if (rowsAffected == 0) {
                System.out.println("No blob_tape entries were deleted.");
            } else {
                System.out.println("Deleted " + rowsAffected + " blob_tape entries.");
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete blob_tape entries", e);
        }
    }

}
