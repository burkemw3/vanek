package burkemw3.vanek;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class Startup {
    private static class CliOptions {
        static String BUCKET_NAME = "bucket-name";
        static String KEY_NAME = "key-name";
        static String FORCE_UPLOAD = "force-upload";

    }

    public static void main(String[] arguments) throws IOException,
            AmazonServiceException, AmazonClientException, InterruptedException {
        AmazonS3 s3 = connectToS3();

        Options options = new Options();
        options.addOption("b", CliOptions.BUCKET_NAME, true, "bucket name");
        options.addOption("k", CliOptions.KEY_NAME, true, "key name");
        options.addOption("f", CliOptions.FORCE_UPLOAD, false,
                "force upload, overwrite existing");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, arguments);

            String bucketName = line.getOptionValue(CliOptions.BUCKET_NAME);
            if (bucketName == null) {
                throw new ParseException("Bucket name must be specified");
            }

            String keyName = line.getOptionValue(CliOptions.KEY_NAME);
            if (keyName == null) {
                throw new ParseException("Folder path must be specified");
            } else if (false == keyName.endsWith(".zip")) {
                throw new ParseException(
                        "key-name should be well formed and end in .zip");
            }

            ensureBucketExists(s3, bucketName);

            Set<File> files = getWorkingDirectoryJpegs();

            boolean forceUpdate = line.hasOption(CliOptions.FORCE_UPLOAD);
            createAndUploadZipFile(s3, bucketName, keyName, files, forceUpdate);

            generateSharingLink(bucketName, keyName);
        } catch (ParseException exp) {
            System.err.println("Parsing failed. Reason: " + exp.getMessage());
        }
    }

    private static AmazonS3 connectToS3() throws FileNotFoundException,
            IOException {
        /* http://aws.amazon.com/security-credentials */
        String homeDirectoryPath = System.getProperty("user.home");
        String awsPropertiesPath = "AwsCredentials.properties";
        File awsCredentials = new File(
                homeDirectoryPath + "/" + awsPropertiesPath);
        FileInputStream fi = new FileInputStream(awsCredentials);
        AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(fi));
        return s3;
    }

    private static void ensureBucketExists(AmazonS3 s3, String bucketName) {
        s3.createBucket(bucketName);
    }

    private static Set<File> getWorkingDirectoryJpegs() {
        String path = System.getProperty("user.dir");
        File root = new File(path);
        Set<File> files = new HashSet<File>();

        for (File file : root.listFiles()) {
            if (false == file.isFile()) {
                continue;
            }
            String filename = file.getName();
            String fileExtension = filename.substring(
                    filename.length() - 3, filename.length());
            if (false == fileExtension.equalsIgnoreCase("jpg")) {
                continue;
            }

            files.add(file);
        }

        return files;
    }

    private static void createAndUploadZipFile(AmazonS3 s3, String bucketName,
            String keyName, Set<File> files, boolean forceUpdate)
            throws IOException, AmazonServiceException, AmazonClientException,
            InterruptedException {
        if (false == forceUpdate) {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName);
            ObjectListing objectListing = s3.listObjects(listObjectsRequest);
            List<S3ObjectSummary> objectSummaries =
                    objectListing.getObjectSummaries();
            for (S3ObjectSummary objectSummary : objectSummaries) {
                if (objectSummary.getKey().equalsIgnoreCase(keyName)) {
                    throw new IOException(
                            "file exists on s3. you can force an upload though");
                }
            }
        }

        String zipFilePath = createZipFile(files);
        File zipFile = new File(zipFilePath);
        try {
            System.out.print("uploading zip...");
            TransferManager tm = new TransferManager(s3);

            PutObjectRequest put =
                    new PutObjectRequest(bucketName, keyName, zipFile);

            put.setStorageClass(StorageClass.ReducedRedundancy);

            put.withCannedAcl(CannedAccessControlList.PublicRead);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/zip");
            put.setMetadata(metadata);

            Upload upload = tm.upload(put);
            upload.waitForCompletion();
            System.out.println("done.");
        } finally {
            zipFile.delete();
        }
    }

    static final int ZIP_BUFFER_SIZE = 2048;

    private static String createZipFile(Set<File> files) throws IOException {
        File file = null;
        try {
            file = File.createTempFile("images", ".zip");
            FileOutputStream dest = new FileOutputStream(file);
            BufferedOutputStream outStream = new BufferedOutputStream(dest);
            ZipOutputStream out = new ZipOutputStream(outStream);
            byte data[] = new byte[ZIP_BUFFER_SIZE];
            int total = files.size();
            System.out.format("Zipping %d files...", total);
            for (File inputFile : files) {
                FileInputStream fi = new FileInputStream(inputFile);
                BufferedInputStream origin =
                        new BufferedInputStream(fi, ZIP_BUFFER_SIZE);
                ZipEntry entry = new ZipEntry(inputFile.getName());
                out.putNextEntry(entry);
                int count;
                while ((count = origin.read(data, 0, ZIP_BUFFER_SIZE)) != -1) {
                    out.write(data, 0, count);
                }
                origin.close();
            }
            out.close();
            System.out.println("done");
        } catch (IOException e) {
            if (null != file) {
                file.delete();
            }
            throw e;
        }

        return file.getAbsolutePath();
    }

    private static void generateSharingLink(String bucketName, String keyName) {
        System.out.println();
        System.out.format(
                "You can download pictures from https://s3.amazonaws.com/%s/%s.\n",
                bucketName, keyName);
    }
}
