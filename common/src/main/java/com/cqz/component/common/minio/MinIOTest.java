package com.cqz.component.common.minio;

import io.minio.*;
import io.minio.errors.MinioException;
import io.minio.http.Method;
import io.minio.messages.Item;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MinIOTest {
    public static void main(String[] args) {
        try {
            // Create a minioClient with the MinIO server playground, its access key and secret key.
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://localhost:9000")
                            .credentials("1II0U6RVM4S6H05MMRCH", "f0ZNZO+hF+hsovRuCyw8Tr0d6qJW01Vc+LX87ZvS")
                            .build();

            // Make 'asiatrip' bucket if not exist.
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket("public").build());
            if (!found) {
                // Make a new bucket called 'asiatrip'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket("public").build());
            } else {
                System.out.println("Bucket 'public' already exists.");
            }

            // Upload '/home/user/Photos/asiaphotos.zip' as object name 'asiaphotos-2015.zip' to bucket
            // 'asiatrip'.

//            minioClient.uploadObject(
//                    UploadObjectArgs.builder()
//                            .bucket("public")
//                            .object("flink-sql-udf-1.0-SNAPSHOT.jar")
//                            .filename("/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/target/flink-sql-udf-1.0-SNAPSHOT.jar")
//                            .build());
//            System.out.println("'/home/cqz/Downloads/credentials.json' is successfully uploaded as " + "object 'credentials.json' to bucket 'public'.");


            try (InputStream input = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("public")
                            .object("path1/111")
                            .build())) {
                File targetFile = new File("/home/cqz/123");
                FileUtils.copyInputStreamToFile(input, targetFile);
            }

            Map<String, String> reqParams = new HashMap<>();
            reqParams.put("response-content-type", "application/json");

            String url =
                    minioClient.getPresignedObjectUrl(
                            GetPresignedObjectUrlArgs.builder()
                                    .method(Method.GET)
                                    .bucket("public")
                                    .object("flink-sql-udf-1.0-SNAPSHOT.jar")
                                    .expiry(2, TimeUnit.HOURS)
                                    .extraQueryParams(reqParams)
                                    .build());
            System.out.println(url);

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket("public")
                            .prefix("path1"+"/"+"flink-1.13.3")
                            .recursive(true)
                            .build()
            );
            for (Result<Item> result : results) {
                if (!result.get().isDir()){
                    System.out.println(result.get().objectName());
                }
            }


        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
            System.out.println("HTTP trace: " + e.httpTrace());
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
}
